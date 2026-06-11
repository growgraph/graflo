"""TigerGraph document and edge data operations."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import requests
from requests import exceptions as requests_exceptions

from graflo.db.conn import consume_insert_edges_kwargs
from graflo.db.tigergraph.document_utils import (
    clean_document,
    extract_id,
    json_serializer_alias as _json_serializer,
)
from graflo.db.tigergraph.gsql_parsers import parse_restpp_response
from graflo.architecture.schema.vertex import FieldType
from graflo.filter.onto import FilterExpression
from graflo.onto import AggregationType
from graflo.util.transform import pick_unique_dict

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def _wrap_tg_exception(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            raise

    return wrapper


class TigerGraphDataOps:
    def __init__(self, conn) -> None:
        self._conn = conn

    def _generate_upsert_payload(
        self, data: list[dict[str, Any]], vname: str, vindex: tuple[str, ...]
    ) -> dict[str, Any]:
        """
        Transforms a list of dictionaries into the TigerGraph REST++ batch upsert JSON format.

        The composite Primary ID is created by concatenating the values of the fields
        specified in vindex with an underscore '_'. Index fields are included in the
        vertex attributes since PRIMARY KEY fields are automatically accessible as
        attributes in TigerGraph queries.

        Attribute values are wrapped in {"value": ...} format as required by TigerGraph REST++ API.

        Args:
            data: List of document dictionaries to upsert
            vname: Target vertex name
            vindex: Tuple of index fields used to create the composite Primary ID

        Returns:
            Dictionary in TigerGraph REST++ batch upsert format:
            {"vertices": {vname: {vertex_id: {attr_name: {"value": attr_value}, ...}}}}
        """
        # Initialize the required JSON structure for vertices
        payload: dict[str, Any] = {"vertices": {vname: {}}}
        vertex_map = payload["vertices"][vname]

        for record in data:
            try:
                # 1. Calculate the Composite Primary ID
                # Assumes all index keys exist in the record
                primary_id_components = [str(record[key]) for key in vindex]
                vertex_id = "_".join(primary_id_components)

                # 2. Clean the record (remove internal keys that shouldn't be stored)
                clean_record = clean_document(record)

                # 3. Keep index fields in attributes
                # When using PRIMARY KEY (composite keys), the key fields are automatically
                # accessible as attributes in queries, so we include them in the payload

                # 4. Format attributes for TigerGraph REST++ API
                # TigerGraph requires attribute values to be wrapped in {"value": ...}
                # Include falsy but valid values (0, False, "") — only None is omitted.
                formatted_attributes = {
                    k: {"value": v} for k, v in clean_record.items() if v is not None
                }

                # 5. Add the record attributes to the map using the composite ID as the key
                vertex_map[vertex_id] = formatted_attributes

            except KeyError as e:
                logger.warning(
                    f"Record is missing a required index field: {e}. Skipping record: {record}"
                )
                continue

        return payload

    def _upsert_data(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Sends the generated JSON payload to the TigerGraph REST++ upsert endpoint.

        Args:
            payload: The JSON payload in TigerGraph REST++ format

        Returns:
            Dictionary containing the response from TigerGraph
        """
        graph_name = self._conn._require_configured_graph_name()

        # Use restpp_url which handles version-specific prefixes (e.g., /restpp for 4.2.1)
        url = f"{self._conn.restpp_url}/graph/{graph_name}"

        # Use centralized auth headers (supports Bearer token for 4.2.1+)
        headers = self._conn._get_auth_headers()
        headers["Content-Type"] = "application/json"

        logger.debug(f"Attempting batch upsert to: {url}")

        try:
            response = requests.post(
                url,
                headers=headers,
                data=json.dumps(payload, default=_json_serializer),
                # Increase timeout for large batches
                timeout=120,
                verify=self._conn.ssl_verify,
            )
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

            # TigerGraph response is a JSON object
            return response.json()

        except requests_exceptions.HTTPError as errh:
            # For TigerGraph 4.2.1, if token auth fails with 401/REST-10018, try Basic Auth fallback
            if (
                errh.response.status_code == 401
                and self._conn.api_token
                and self._conn.config.username
                and self._conn.config.password
                and "REST-10018" in str(errh)
            ):
                logger.warning(
                    "Token authentication failed with REST-10018, "
                    "falling back to Basic Auth for TigerGraph 4.2.1 compatibility"
                )
                # Retry with Basic Auth
                import base64

                credentials = (
                    f"{self._conn.config.username}:{self._conn.config.password}"
                )
                encoded_credentials = base64.b64encode(credentials.encode()).decode()
                headers["Authorization"] = f"Basic {encoded_credentials}"
                try:
                    response = requests.post(
                        url,
                        headers=headers,
                        data=json.dumps(payload, default=_json_serializer),
                        timeout=120,
                        verify=self._conn.ssl_verify,
                    )
                    response.raise_for_status()
                    logger.info("Successfully authenticated using Basic Auth fallback")
                    return response.json()
                except requests_exceptions.HTTPError as errh2:
                    logger.error(f"HTTP Error (after Basic Auth fallback): {errh2}")
                    error_details = ""
                    try:
                        error_details = response.text
                    except Exception:
                        pass
                    return {
                        "error": True,
                        "message": str(errh2),
                        "details": error_details,
                    }

            logger.error(f"HTTP Error: {errh}")
            error_details = ""
            try:
                error_details = response.text
            except Exception:
                pass
            return {"error": True, "message": str(errh), "details": error_details}
        except requests_exceptions.ConnectionError as errc:
            logger.error(f"Error Connecting: {errc}")
            return {"error": True, "message": str(errc)}
        except requests_exceptions.Timeout as errt:
            logger.error(f"Timeout Error: {errt}")
            return {"error": True, "message": str(errt)}
        except requests_exceptions.RequestException as err:
            logger.error(f"An unexpected error occurred: {err}")
            return {"error": True, "message": str(err)}

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
        """
        Batch upsert documents as vertices using TigerGraph REST++ API.

        Creates a GSQL job and formats the payload for batch upsert operations.
        Uses composite Primary IDs constructed from match_keys.
        """
        dry = kwargs.pop("dry", False)
        if dry:
            logger.debug(f"Dry run: would upsert {len(docs)} documents to {class_name}")
            return

        try:
            # Convert match_keys to tuple if it's a list
            vindex = tuple(match_keys) if isinstance(match_keys, list) else match_keys

            # Generate the upsert payload
            payload = self._generate_upsert_payload(docs, class_name, vindex)

            # Check if payload has any vertices
            if not payload.get("vertices", {}).get(class_name):
                logger.warning(f"No valid vertices to upsert for {class_name}")
                return

            # Send the upsert request
            result = self._upsert_data(payload)

            if result.get("error"):
                logger.error(
                    f"Error upserting vertices to {class_name}: {result.get('message')}"
                )
            else:
                num_vertices = len(payload["vertices"][class_name])
                logger.debug(
                    f"Upserted {num_vertices} vertices to {class_name}: {result}"
                )
                return result

        except Exception as e:
            logger.error(f"Error upserting vertices to {class_name}: {e}")

    def _generate_edge_upsert_payloads(
        self,
        edges_data: list[tuple[dict, dict, dict]],
        source_class: str,
        target_class: str,
        edge_type: str,
        match_keys_source: tuple[str, ...],
        match_keys_target: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        """
        Transforms edge data into multiple TigerGraph REST++ batch upsert JSON payloads.

        Groups edges by (source_id, target_id, edge_type) and collects all weight combinations
        for each triple. Then creates separate payloads by "zipping" the weight lists across
        all (source_id, target_id, edge_type) groups.

        Args:
            edges_data: List of tuples (source_doc, target_doc, edge_props)
            source_class: Source vertex type name
            target_class: Target vertex type name
            edge_type: Edge type/relation name (e.g., "relates")
            match_keys_source: Tuple of index fields for source vertex
            match_keys_target: Tuple of index fields for target vertex

        Returns:
            List of payload dictionaries in TigerGraph REST++ format:
            [{"edges": {source_v_type: {source_id: {edge_type: {target_v_type: {target_id: attributes}}}}}}, ...]
        """
        from collections import defaultdict

        # Step 1: Group edges by (source_id, target_id, edge_type) and collect weight combinations
        # Structure: {(source_id, target_id, edge_type): [weight_dict1, weight_dict2, ...]}
        uvr_weights_map: defaultdict[tuple[str, str, str], list[dict]] = defaultdict(
            list
        )

        # Also track original edge data for fallback
        uvr_edges_map: defaultdict[
            tuple[str, str, str], list[tuple[dict, dict, dict]]
        ] = defaultdict(list)

        for source_doc, target_doc, edge_props in edges_data:
            try:
                # Extract IDs
                source_id = extract_id(source_doc, match_keys_source)
                target_id = extract_id(target_doc, match_keys_target)

                if not source_id or not target_id:
                    logger.warning(
                        f"Missing source_id ({source_id}) or target_id ({target_id}) for edge"
                    )
                    continue

                # Clean and format edge attributes
                clean_edge_props = clean_document(edge_props)
                formatted_attributes = {
                    k: {"value": v} for k, v in clean_edge_props.items() if v
                }

                # Group by (source_id, target_id, edge_type)
                # edge_type is the actual edge type name (e.g., "relates"), not a weight value
                uvr_key = (source_id, target_id, edge_type)
                uvr_weights_map[uvr_key].append(formatted_attributes)
                uvr_edges_map[uvr_key].append((source_doc, target_doc, edge_props))

            except Exception as e:
                logger.error(f"Error processing edge: {e}")
                continue

        # Step 2: Find the maximum number of weights across all (u, v, r) groups
        # This determines how many payloads we need to create (k payloads for k max elements)
        max_weights = (
            max(len(weights_list) for weights_list in uvr_weights_map.values())
            if uvr_weights_map
            else 0
        )

        if max_weights == 0:
            return []

        # Step 3: Create k payloads by "zipping" weight lists across all (u, v, r) groups
        # Unlike Python's zip() which stops at the shortest iterable, we create k payloads
        # where k is the maximum group size. Payload i contains element i from each group
        # (if that group has an element at index i).
        payloads = []
        for weight_idx in range(max_weights):
            payload: dict[str, Any] = {"edges": {source_class: {}}}
            source_map = payload["edges"][source_class]
            payload_original_edges = []

            # Iterate through all (u, v, r) groups and take element at weight_idx
            for uvr_key, weights_list in uvr_weights_map.items():
                # Skip if this group doesn't have a weight at this index
                if weight_idx >= len(weights_list):
                    continue

                source_id, target_id, edge_type_key = uvr_key
                weight_attrs = weights_list[weight_idx]
                original_edge = uvr_edges_map[uvr_key][weight_idx]

                # Build nested structure
                if source_id not in source_map:
                    source_map[source_id] = {edge_type: {}}

                if edge_type not in source_map[source_id]:
                    source_map[source_id][edge_type] = {target_class: {}}

                if target_class not in source_map[source_id][edge_type]:
                    source_map[source_id][edge_type][target_class] = {}

                target_map = source_map[source_id][edge_type][target_class]

                # Add edge at this index from this (u, v, r) group
                target_map[target_id] = weight_attrs
                payload_original_edges.append(original_edge)

            # Only add payload if it has edges (skip empty payloads)
            if payload_original_edges:
                payload["_original_edges"] = payload_original_edges
                payloads.append(payload)

        return payloads

    def _fallback_individual_edge_upsert(
        self,
        edges_data: list[tuple[dict, dict, dict]],
        source_class: str,
        target_class: str,
        edge_type: str,
        match_keys_source: tuple[str, ...],
        match_keys_target: tuple[str, ...],
    ) -> None:
        """Fallback method for individual edge upserts.

        Args:
            edges_data: List of tuples (source_doc, target_doc, edge_props)
            source_class: Source vertex type name
            target_class: Target vertex type name
            edge_type: Edge type name
            match_keys_source: Keys for source vertex ID
            match_keys_target: Keys for target vertex ID
        """
        for source_doc, target_doc, edge_props in edges_data:
            try:
                source_id = extract_id(source_doc, match_keys_source)
                target_id = extract_id(target_doc, match_keys_target)

                if source_id and target_id:
                    clean_edge_props = clean_document(edge_props)
                    # Serialize data for REST API
                    serialized_props = json.loads(
                        json.dumps(clean_edge_props, default=_json_serializer)
                    )
                    self._conn._upsert_edge(
                        source_class,
                        source_id,
                        edge_type,
                        target_class,
                        target_id,
                        serialized_props,
                    )
            except Exception as e:
                logger.error(f"Error upserting individual edge: {e}")

    def insert_edges_batch(
        self,
        docs_edges: list[list[dict[str, Any]]] | list[Any] | None,
        source_class: str,
        target_class: str,
        relation_name: str,
        match_keys_source: tuple[str, ...],
        match_keys_target: tuple[str, ...],
        filter_uniques: bool = True,
        head: int | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Batch insert/upsert edges using TigerGraph REST++ API.

        Handles edge data in tuple format: [(source_doc, target_doc, edge_props), ...]
        or dict format: [{"_source_aux": {...}, "_target_aux": {...}, "_edge_props": {...}}, ...]

        Args:
            docs_edges: List of edge documents (tuples or dicts)
            source_class: Source vertex type name
            target_class: Target vertex type name
            relation_name: Edge type/relation name
            match_keys_source: Keys to match source vertices
            match_keys_target: Keys to match target vertices
            filter_uniques: If True, filter duplicate edges (used)
            head: Optional limit on number of edges to insert (used)
            **kwargs: Additional options:
                - dry: If True, don't execute the query
                - collection_name: Alternative edge type name (used if relation_name is None)
                - uniq_weight_fields: Unused in TigerGraph (ArangoDB-specific)
                - uniq_weight_collections: Unused in TigerGraph (ArangoDB-specific)
                - on_duplicate: Unused in TigerGraph (ArangoDB-specific AQL policy)
                - relationship_merge_properties: Unused (Cypher property-graph backends only)
        """
        opts = consume_insert_edges_kwargs(kwargs)
        dry = opts.dry
        collection_name = opts.collection_name
        if dry:
            if docs_edges is not None:
                logger.debug(f"Dry run: would insert {len(docs_edges)} edges")
            return

        # Process edges list
        if isinstance(docs_edges, list):
            if head is not None:
                docs_edges = docs_edges[:head]
            if filter_uniques:
                docs_edges = pick_unique_dict(docs_edges)

        # Normalize edge data format - handle both tuple and dict formats
        if docs_edges is None:
            return
        normalized_edges = []
        for edge_item in docs_edges:
            try:
                if isinstance(edge_item, tuple) and len(edge_item) == 3:
                    # Tuple format: (source_doc, target_doc, edge_props)
                    source_doc, target_doc, edge_props = edge_item
                    normalized_edges.append((source_doc, target_doc, edge_props))
                elif isinstance(edge_item, dict):
                    # Dict format: {"_source_aux": {...}, "_target_aux": {...}, "_edge_props": {...}}
                    source_doc = edge_item.get("_source_aux", {})
                    target_doc = edge_item.get("_target_aux", {})
                    edge_props = edge_item.get("_edge_props", {})
                    normalized_edges.append((source_doc, target_doc, edge_props))
                else:
                    logger.warning(f"Unexpected edge format: {edge_item}")
            except Exception as e:
                logger.error(f"Error normalizing edge item: {e}")
                continue

        if not normalized_edges:
            logger.warning("No valid edges to insert")
            return

        resolved_edge_type = (relation_name or collection_name or "").strip()
        if not resolved_edge_type:
            logger.error(
                "Edge type must be specified via relation_name or collection_name"
            )
            return

        try:
            # Convert match_keys to tuples if they're lists
            match_keys_src = (
                tuple(match_keys_source)
                if isinstance(match_keys_source, list)
                else match_keys_source
            )
            match_keys_tgt = (
                tuple(match_keys_target)
                if isinstance(match_keys_target, list)
                else match_keys_target
            )

            edge_type = resolved_edge_type

            # Generate multiple edge upsert payloads (one per unique attribute combination)
            payloads = self._generate_edge_upsert_payloads(
                normalized_edges,
                source_class,
                target_class,
                edge_type,
                match_keys_src,
                match_keys_tgt,
            )

            if not payloads:
                logger.warning(f"No valid edges to upsert for edge type {edge_type}")
                return

            # Send each payload in batch
            total_edges = 0
            failed_payloads = []
            for i, payload in enumerate(payloads):
                edges_payload = payload.get("edges", {})
                if not edges_payload or source_class not in edges_payload:
                    continue

                # Store original edges for fallback before removing metadata
                original_edges = payload.pop("_original_edges", [])

                # Send the batch upsert request
                result = self._upsert_data(payload)

                # Restore original edges for potential fallback
                payload["_original_edges"] = original_edges

                if result.get("error"):
                    logger.error(
                        f"Error upserting edges of type {edge_type} (payload {i + 1}/{len(payloads)}): "
                        f"{result.get('message')}"
                    )
                    # Collect failed payload for fallback
                    failed_payloads.append((payload, i))
                else:
                    # Count edges in this payload
                    edge_count = 0
                    for source_id_map in edges_payload[source_class].values():
                        if edge_type in source_id_map:
                            for target_type_map in source_id_map[edge_type].values():
                                for attrs_or_list in target_type_map.values():
                                    if isinstance(attrs_or_list, list):
                                        edge_count += len(attrs_or_list)
                                    else:
                                        edge_count += 1
                    total_edges += edge_count
                    logger.debug(
                        f"Upserted {edge_count} edges of type {edge_type} via batch "
                        f"(payload {i + 1}/{len(payloads)}): {result}"
                    )

            # Handle failed payloads with individual upserts
            if failed_payloads:
                logger.warning(
                    f"{len(failed_payloads)} payload(s) failed, falling back to individual upserts"
                )
                # Extract original edges from failed payloads for individual upsert
                failed_edges = []
                for payload, _ in failed_payloads:
                    # Use the stored original edges for this payload
                    original_edges = payload.get("_original_edges", [])
                    failed_edges.extend(original_edges)

                if failed_edges:
                    logger.debug(
                        f"Sending {len(failed_edges)} edges from failed payloads via individual upserts"
                    )
                    self._fallback_individual_edge_upsert(
                        failed_edges,
                        source_class,
                        target_class,
                        edge_type,
                        match_keys_src,
                        match_keys_tgt,
                    )

            logger.debug(
                f"Total upserted {total_edges} edges of type {edge_type} across {len(payloads)} payloads"
            )
            return

        except Exception as e:
            logger.error(f"Error batch inserting edges: {e}")
            # Fallback to individual operations
            m_src = (
                tuple(match_keys_source)
                if isinstance(match_keys_source, list)
                else match_keys_source
            )
            m_tgt = (
                tuple(match_keys_target)
                if isinstance(match_keys_target, list)
                else match_keys_target
            )
            self._fallback_individual_edge_upsert(
                normalized_edges,
                source_class,
                target_class,
                resolved_edge_type,
                m_src,
                m_tgt,
            )

    def insert_return_batch(
        self, docs: list[dict[str, Any]], class_name: str
    ) -> list[dict[str, Any]] | str:
        """
        TigerGraph doesn't have INSERT...RETURN semantics like ArangoDB.
        """
        raise NotImplementedError(
            "insert_return_batch not supported in TigerGraph - use upsert_docs_batch instead"
        )

    def _render_rest_filter(
        self,
        filters: list | dict | FilterExpression | None,
        field_types: dict[str, FieldType] | None = None,
    ) -> str:
        """Convert filter expressions to REST++ filter format.

        REST++ filter format: "field=value" or "field>value" etc.
        Format: fieldoperatorvalue (no spaces, quotes for string values)
        Example: "hindex=10" or "hindex>20" or 'name="John"'

        Args:
            filters: Filter expression to convert
            field_types: Optional mapping of field names to FieldType enum values

        Returns:
            str: REST++ filter string (empty if no filters)
        """
        if filters is not None:
            if not isinstance(filters, FilterExpression):
                ff = FilterExpression.from_dict(filters)
            else:
                ff = filters

            # Use GSQL flavor with empty doc_name to trigger REST++ format
            # Pass field_types to help with proper value quoting
            result = ff(
                doc_name="",
                kind=self._conn.expression_flavor(),
                field_types=field_types,
            )
            return result if isinstance(result, str) else ""
        else:
            return ""

    def fetch_docs(
        self,
        class_name: str,
        filters: list[Any] | dict[str, Any] | FilterExpression | None = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """
        Fetch documents (vertices) with filtering and projection using REST++ API.

        Args:
            class_name: Vertex type name (or dbname)
            filters: Filter expression (list, dict, or FilterExpression)
            limit: Maximum number of documents to return
            return_keys: Keys to return (projection)
            unset_keys: Keys to exclude (projection)
            **kwargs: Additional parameters
                field_types: Optional mapping of field names to FieldType enum values
                           Used to properly quote string values in filters
                           If not provided and vertex_config is provided, will be auto-detected
                vertex_config: Optional VertexConfig object to use for field type lookup

        Returns:
            list: List of fetched documents
        """
        try:
            graph_name = self._conn._require_configured_graph_name()

            # Get field_types from kwargs or auto-detect from vertex_config
            field_types = kwargs.get("field_types")
            vertex_config = kwargs.get("vertex_config")

            if field_types is None and vertex_config is not None:
                field_types = {
                    f.name: f.type for f in vertex_config.properties(class_name)
                }

            # Build REST++ filter string with field type information
            filter_str = self._render_rest_filter(filters, field_types=field_types)

            # Build REST++ API endpoint with query parameters manually
            # Format: /graph/{graph_name}/vertices/{vertex_type}?filter=...&limit=...
            # Example: /graph/g22c97325/vertices/Author?filter=hindex>20&limit=10

            endpoint = f"/graph/{graph_name}/vertices/{class_name}"
            query_parts = []

            if filter_str:
                # URL-encode the filter string to handle special characters
                encoded_filter = quote(filter_str, safe="=<>!&|")
                query_parts.append(f"filter={encoded_filter}")
            if limit is not None:
                query_parts.append(f"limit={limit}")

            if query_parts:
                endpoint = f"{endpoint}?{'&'.join(query_parts)}"

            logger.debug(f"Calling REST++ API: {endpoint}")

            # Call REST++ API directly (no params dict, we built the URL ourselves)
            response = self._conn._call_restpp_api(endpoint)

            # Parse REST++ response (vertices only)
            result: list[dict[str, Any]] = parse_restpp_response(
                response, is_edge=False
            )

            # Check for errors
            if isinstance(response, dict) and response.get("error"):
                raise Exception(
                    f"REST++ API error: {response.get('message', response)}"
                )

            # Apply projection (client-side projection is acceptable for result formatting)
            if return_keys is not None:
                result = [
                    {k: doc.get(k) for k in return_keys if k in doc}
                    for doc in result
                    if isinstance(doc, dict)
                ]
            elif unset_keys is not None:
                result = [
                    {k: v for k, v in doc.items() if k not in unset_keys}
                    for doc in result
                    if isinstance(doc, dict)
                ]

            return result

        except Exception as e:
            logger.error(f"Error fetching documents from {class_name} via REST++: {e}")
            raise

    def fetch_edges(
        self,
        from_type: str,
        from_id: str,
        edge_type: str | None = None,
        to_type: str | None = None,
        to_id: str | None = None,
        filters: list[Any] | dict[str, Any] | FilterExpression | None = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """
        Fetch edges from TigerGraph using REST API.

        In TigerGraph, you must know at least one vertex ID before you can fetch edges.
        Uses REST API which handles special characters in vertex IDs.

        Args:
            from_type: Source vertex type (required)
            from_id: Source vertex ID (required)
            edge_type: Optional edge type to filter by
            to_type: Optional target vertex type to filter by (not used in REST API)
            to_id: Optional target vertex ID to filter by (not used in REST API)
            filters: Additional query filters (not supported by REST API)
            limit: Maximum number of edges to return (not supported by REST API)
            return_keys: Keys to return (projection)
            unset_keys: Keys to exclude (projection)
            **kwargs: Additional parameters

        Returns:
            list: List of fetched edges
        """
        try:
            if not from_type or not from_id:
                raise ValueError(
                    "from_type and from_id are required for fetching edges in TigerGraph"
                )

            # Use REST API to get edges
            # Returns: list of edge dictionaries
            logger.debug(
                f"Fetching edges using REST API: from_type={from_type}, from_id={from_id}, edge_type={edge_type}"
            )

            # Handle None edge_type
            edge_type_str = edge_type if edge_type is not None else None
            edges = self._conn._get_edges(from_type, from_id, edge_type_str)

            # Parse REST API response format
            # _get_edges() returns list of edge dicts from REST++ API
            # Format: [{"e_type": "...", "from_id": "...", "to_id": "...", "attributes": {...}}, ...]
            # The REST API returns edges in a flat format with e_type, from_id, to_id, attributes
            if isinstance(edges, list):
                # Process each edge to normalize format
                result = []
                for edge in edges:
                    if isinstance(edge, dict):
                        # Normalize edge format - REST API returns flat structure
                        normalized_edge = {}

                        # Extract edge type (rename e_type to edge_type for consistency)
                        normalized_edge["edge_type"] = edge.get(
                            "e_type", edge.get("edge_type", "")
                        )

                        # Extract from/to IDs and types
                        normalized_edge["from_id"] = edge.get("from_id", "")
                        normalized_edge["from_type"] = edge.get("from_type", "")
                        normalized_edge["to_id"] = edge.get("to_id", "")
                        normalized_edge["to_type"] = edge.get("to_type", "")

                        # Handle nested "from"/"to" objects if present (some API versions)
                        if "from" in edge and isinstance(edge["from"], dict):
                            normalized_edge["from_id"] = edge["from"].get(
                                "id",
                                edge["from"].get("v_id", normalized_edge["from_id"]),
                            )
                            normalized_edge["from_type"] = edge["from"].get(
                                "type",
                                edge["from"].get(
                                    "v_type", normalized_edge["from_type"]
                                ),
                            )

                        if "to" in edge and isinstance(edge["to"], dict):
                            normalized_edge["to_id"] = edge["to"].get(
                                "id", edge["to"].get("v_id", normalized_edge["to_id"])
                            )
                            normalized_edge["to_type"] = edge["to"].get(
                                "type",
                                edge["to"].get("v_type", normalized_edge["to_type"]),
                            )

                        # Extract attributes and merge into normalized edge
                        attributes = edge.get("attributes", {})
                        if attributes:
                            normalized_edge.update(attributes)
                        else:
                            # If no attributes key, include all other fields as attributes
                            for k, v in edge.items():
                                if k not in (
                                    "e_type",
                                    "edge_type",
                                    "from",
                                    "to",
                                    "from_id",
                                    "to_id",
                                    "from_type",
                                    "to_type",
                                    "directed",
                                ):
                                    normalized_edge[k] = v

                        result.append(normalized_edge)
            elif isinstance(edges, dict):
                # Single edge dict - normalize and wrap in list
                normalized_edge = {}
                normalized_edge["edge_type"] = edges.get(
                    "e_type", edges.get("edge_type", "")
                )
                normalized_edge["from_id"] = edges.get("from_id", "")
                normalized_edge["to_id"] = edges.get("to_id", "")

                if "from" in edges and isinstance(edges["from"], dict):
                    normalized_edge["from_id"] = edges["from"].get(
                        "id", edges["from"].get("v_id", normalized_edge["from_id"])
                    )
                if "to" in edges and isinstance(edges["to"], dict):
                    normalized_edge["to_id"] = edges["to"].get(
                        "id", edges["to"].get("v_id", normalized_edge["to_id"])
                    )

                attributes = edges.get("attributes", {})
                if attributes:
                    normalized_edge.update(attributes)
                else:
                    for k, v in edges.items():
                        if k not in (
                            "e_type",
                            "edge_type",
                            "from",
                            "to",
                            "from_id",
                            "to_id",
                        ):
                            normalized_edge[k] = v

                result = [normalized_edge]
            else:
                # Fallback for unexpected types
                result: list[dict[str, Any]] = []
                logger.debug(f"Unexpected edges type: {type(edges)}")

            # Apply limit if specified (client-side since REST API doesn't support it)
            if limit is not None and limit > 0:
                result = result[:limit]

            # Apply projection (client-side projection is acceptable for result formatting)
            if return_keys is not None:
                result = [
                    {k: doc.get(k) for k in return_keys if k in doc}
                    for doc in result
                    if isinstance(doc, dict)
                ]
            elif unset_keys is not None:
                result = [
                    {k: v for k, v in doc.items() if k not in unset_keys}
                    for doc in result
                    if isinstance(doc, dict)
                ]

            return result

        except Exception as e:
            logger.error(f"Error fetching edges via REST API: {e}")
            raise

    def fetch_present_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        flatten: bool = False,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Check which documents from batch are present in the database.
        """
        try:
            present_docs: list[dict[str, Any]] = []
            keep_keys_list: list[str] | tuple[str, ...] = (
                list(keep_keys) if keep_keys is not None else []
            )
            if isinstance(keep_keys_list, tuple):
                keep_keys_list = list(keep_keys_list)

            for doc in batch:
                vertex_id = extract_id(doc, match_keys)
                if not vertex_id:
                    continue

                try:
                    vertex_data = self._conn._get_vertices_by_id(class_name, vertex_id)
                    if vertex_data and vertex_id in vertex_data:
                        # Extract requested keys
                        vertex_attrs = vertex_data[vertex_id].get("attributes", {})
                        filtered_doc: dict[str, Any] = {}

                        if keep_keys_list:
                            for key in keep_keys_list:
                                if key == "id":
                                    filtered_doc[key] = vertex_id
                                elif key in vertex_attrs:
                                    filtered_doc[key] = vertex_attrs[key]
                        else:
                            # If no keep_keys specified, return all attributes
                            filtered_doc = vertex_attrs.copy()
                            filtered_doc["id"] = vertex_id

                        present_docs.append(filtered_doc)

                except Exception:
                    # Vertex doesn't exist or error occurred
                    continue

            return present_docs

        except Exception as e:
            logger.error(f"Error fetching present documents: {e}")
            return []

    def aggregate(
        self,
        class_name,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: list | dict | None = None,
    ):
        """
        Perform aggregation operations.
        """
        try:
            if aggregation_function == AggregationType.COUNT and discriminant is None:
                # Simple vertex count
                count = self._conn._get_vertex_count(class_name)
                return [{"_value": count}]
            else:
                # Complex aggregations require custom GSQL queries
                logger.warning(
                    f"Complex aggregation {aggregation_function} requires custom GSQL implementation"
                )
                return []
        except Exception as e:
            logger.error(f"Error in aggregation: {e}")
            return []

    def keep_absent_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Return documents from batch that are NOT present in database.
        """
        present_docs = self.fetch_present_documents(
            batch=batch,
            class_name=class_name,
            match_keys=match_keys,
            keep_keys=keep_keys,
            flatten=False,
            filters=filters,
        )

        # Create a set of IDs from present documents for efficient lookup
        present_ids = set()
        for present_doc in present_docs:
            # Extract ID from present document (it should have 'id' key)
            if "id" in present_doc:
                present_ids.add(present_doc["id"])

        # Find documents that are not present
        absent_docs: list[dict[str, Any]] = []
        keep_keys_list: list[str] | tuple[str, ...] = (
            list(keep_keys) if keep_keys is not None else []
        )
        if isinstance(keep_keys_list, tuple):
            keep_keys_list = list(keep_keys_list)

        for doc in batch:
            vertex_id = extract_id(doc, match_keys)
            if not vertex_id or vertex_id not in present_ids:
                if keep_keys_list:
                    # Filter to keep only requested keys
                    filtered_doc = {k: doc.get(k) for k in keep_keys_list if k in doc}
                    absent_docs.append(filtered_doc)
                else:
                    absent_docs.append(doc)

        return absent_docs
