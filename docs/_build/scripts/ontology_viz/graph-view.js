(function () {
  "use strict";

  const data = window.GRAFLO_ONTOLOGY_GRAPH;
  if (!data) {
    return;
  }

  const LAYOUT = {
    // Wide enough for taxonomy edges to route between columns...
    colGap: 56,
    // ...but classes with no taxonomy at all are packed tight, since nothing
    // has to be drawn in the gap.
    gridGap: 16,
    rowGap: 16,
    bandPadX: 20,
    bandPadY: 14,
    bandGap: 26,
    bandHeader: 24,
    // Grid widths tried for the flat (no-subClassOf) part of each band.
    gridColsCandidates: [2, 3, 4, 5, 6, 7, 8],
    // The embed frame the docs page reserves (1400x720 less its chrome), so the
    // arrangement search optimises for the viewport the picture is actually
    // first seen in rather than a nominal 16:9.
    targetAspect: 2.04,
  };

  // Group colours are addressed by hue so no group name ever appears in CSS or
  // JS; bands and accents read `--group-hue` off the element.
  const GROUP_HUES = [210, 145, 28, 275, 340, 190, 60, 12];

  // Ignore sub-pixel jitter so tap/click can select nodes instead of being treated as drag.
  const DRAG_THRESHOLD_PX = 5;

  const SVG_NS = "http://www.w3.org/2000/svg";
  const colStride = data.nodeWidth + LAYOUT.colGap;
  const gridStride = data.nodeWidth + LAYOUT.gridGap;
  const rowStride = data.nodeHeight + LAYOUT.rowGap;

  const svg = document.getElementById("graph");
  const viewport = document.createElementNS(SVG_NS, "g");
  svg.appendChild(viewport);

  const defs = document.createElementNS(SVG_NS, "defs");
  [
    ["subClassOf", "#546e7a", 8],
    ["equivalentClass", "#009688", 7],
    ["objectProperty", "#1565c0", 6],
  ].forEach(function (entry) {
    const marker = document.createElementNS(SVG_NS, "marker");
    marker.setAttribute("id", "arrow-" + entry[0]);
    marker.setAttribute("viewBox", "0 -4 8 8");
    marker.setAttribute("refX", 7);
    marker.setAttribute("refY", 0);
    marker.setAttribute("markerWidth", entry[2]);
    marker.setAttribute("markerHeight", entry[2]);
    marker.setAttribute("orient", "auto");
    const path = document.createElementNS(SVG_NS, "path");
    path.setAttribute("d", "M0,-4L8,0L0,4");
    path.setAttribute("fill", entry[1]);
    marker.appendChild(path);
    defs.appendChild(marker);
  });
  svg.insertBefore(defs, viewport);

  const nodeById = new Map(
    data.nodes.map(function (node) {
      return [node.id, node];
    }),
  );
  const groups = data.groups && data.groups.length ? data.groups : [{ id: "core", label: "Core" }];
  const groupIndex = new Map(
    groups.map(function (group, index) {
      return [group.id, index];
    }),
  );
  // Every subClassOf edge, including the ones the viewer suppresses — the details
  // panel reports the true taxonomy even when it is not drawn.
  const allSubclassEdges = data.edges.filter(function (edge) {
    return edge.kind === "subClassOf";
  });
  // Composition. Only the layout reads these; whether they are *drawn* is still
  // decided by the relation filter and the current selection.
  const objectPropertyEdges = data.edges.filter(function (edge) {
    return edge.kind === "objectProperty";
  });
  const rootNode = data.universalRoot ? nodeById.get(data.universalRoot) || null : null;

  const state = {
    scale: 1,
    tx: 40,
    ty: 40,
    selectedId: null,
    search: "",
    relationMode: "taxonomy",
    showUniversalRoot: false,
    draggingViewport: false,
    draggingNodeId: null,
    dragMoved: false,
    pointerDownX: 0,
    pointerDownY: 0,
    lastX: 0,
    lastY: 0,
  };

  let bandBoxes = [];

  function groupHue(groupId) {
    const index = groupIndex.has(groupId) ? groupIndex.get(groupId) : 0;
    return GROUP_HUES[index % GROUP_HUES.length];
  }

  function pointerTravelPx(event) {
    const dx = event.clientX - state.pointerDownX;
    const dy = event.clientY - state.pointerDownY;
    return Math.hypot(dx, dy);
  }

  function markDragIfNeeded(event) {
    if (!state.dragMoved && pointerTravelPx(event) >= DRAG_THRESHOLD_PX) {
      state.dragMoved = true;
    }
  }

  function truncate(text, max) {
    if (text.length <= max) {
      return text;
    }
    return text.slice(0, max - 1) + "…";
  }

  // ---------------------------------------------------------------- suppression

  function rootIsHidden() {
    return Boolean(data.universalRoot) && !state.showUniversalRoot;
  }

  function nodeIsHidden(node) {
    return rootIsHidden() && node.id === data.universalRoot;
  }

  function edgeIsSuppressed(edge) {
    return (
      rootIsHidden() &&
      (edge.source === data.universalRoot || edge.target === data.universalRoot)
    );
  }

  function layoutNodes() {
    return data.nodes.filter(function (node) {
      return !nodeIsHidden(node);
    });
  }

  // The taxonomy that drives layout *and* rendering, so toggling the root
  // re-lays-out rather than merely hiding paths.
  function layoutEdges() {
    return allSubclassEdges.filter(function (edge) {
      return !edgeIsSuppressed(edge);
    });
  }

  // ---------------------------------------------------------------- layout

  // How a band's classes refine one another, as `child -> parent` pairs.
  //
  // Taxonomy alone is not enough: once the universal root is suppressed the
  // Schema band has *no* internal subClassOf edge at all, and its 17 classes
  // would fall into an arbitrary grid. Its hierarchy is real, it is just written
  // as composition — `Schema hasCoreSchema CoreSchema hasVertexConfig
  // VertexConfig hasVertex Vertex hasField Field` — so an object property's
  // range points back at its domain and ranks one step to its right.
  //
  // Cross-band edges are excluded, exactly as `componentsOf` already excludes
  // them: depth is band-local, so the PROV-O links do not drag a class sideways.
  function rankEdgesForBand(bandNodes, taxonomyEdges) {
    const ids = new Set(
      bandNodes.map(function (node) {
        return node.id;
      }),
    );
    const seen = new Set();
    const edges = [];
    function push(child, parent) {
      if (child === parent || !ids.has(child) || !ids.has(parent)) {
        return;
      }
      const key = child + " " + parent;
      if (seen.has(key)) {
        return;
      }
      seen.add(key);
      edges.push({ source: child, target: parent });
    }
    taxonomyEdges.forEach(function (edge) {
      push(edge.source, edge.target);
    });
    objectPropertyEdges.forEach(function (edge) {
      push(edge.target, edge.source);
    });
    return edges;
  }

  // Two passes over the rank graph.
  //
  // First the *shortest* distance from the band's roots, so peers stay peers:
  // `Vertex` and `Edge` are both three steps from `Schema`, and the `edgeSource`
  // reference between them must not push one behind the other — which a
  // longest-path pass would do.
  //
  // Then relax taxonomy to a fixpoint, so a subclass is always strictly right of
  // its superclass however short its composition route happens to be.
  function assignDepths(nodes, rankEdges, taxonomyEdges) {
    const parentCount = new Map();
    const children = new Map();
    nodes.forEach(function (node) {
      parentCount.set(node.id, 0);
      children.set(node.id, []);
    });
    rankEdges.forEach(function (edge) {
      parentCount.set(edge.source, parentCount.get(edge.source) + 1);
      children.get(edge.target).push(edge.source);
    });
    children.forEach(function (kids, key) {
      children.set(key, kids.slice().sort());
    });

    const depth = new Map();
    const queue = [];
    nodes
      .slice()
      .sort(byLabel)
      .forEach(function (node) {
        if (!parentCount.get(node.id)) {
          depth.set(node.id, 0);
          queue.push(node.id);
        }
      });
    for (let head = 0; head < queue.length; head += 1) {
      const id = queue[head];
      children.get(id).forEach(function (kid) {
        if (!depth.has(kid)) {
          depth.set(kid, depth.get(id) + 1);
          queue.push(kid);
        }
      });
    }
    // Only reachable from inside a cycle, so never enqueued. Anchor at 0 rather
    // than dropping the class off the canvas.
    nodes.forEach(function (node) {
      if (!depth.has(node.id)) {
        depth.set(node.id, 0);
      }
    });

    const inBand = taxonomyEdges.filter(function (edge) {
      return depth.has(edge.source) && depth.has(edge.target);
    });
    for (let pass = 0; pass < nodes.length; pass += 1) {
      let changed = false;
      inBand.forEach(function (edge) {
        const want = depth.get(edge.target) + 1;
        if (depth.get(edge.source) < want) {
          depth.set(edge.source, want);
          changed = true;
        }
      });
      if (!changed) {
        break;
      }
    }
    return depth;
  }

  function byLabel(a, b) {
    if (a.label === b.label) {
      return a.id < b.id ? -1 : 1;
    }
    return a.label < b.label ? -1 : 1;
  }

  // Connected components of a band under its *internal* taxonomy edges.
  // Cross-band edges (the PROV-O links) are drawn but do not drive slotting.
  function componentsOf(bandNodes, edges) {
    const ids = new Set(
      bandNodes.map(function (node) {
        return node.id;
      }),
    );
    const parent = new Map();
    ids.forEach(function (id) {
      parent.set(id, id);
    });
    function find(id) {
      while (parent.get(id) !== id) {
        parent.set(id, parent.get(parent.get(id)));
        id = parent.get(id);
      }
      return id;
    }
    edges.forEach(function (edge) {
      if (!ids.has(edge.source) || !ids.has(edge.target)) {
        return;
      }
      const a = find(edge.source);
      const b = find(edge.target);
      if (a !== b) {
        parent.set(a, b);
      }
    });
    const buckets = new Map();
    bandNodes.forEach(function (node) {
      const key = find(node.id);
      if (!buckets.has(key)) {
        buckets.set(key, []);
      }
      buckets.get(key).push(node);
    });
    return Array.from(buckets.values()).map(function (members) {
      return members.slice().sort(byLabel);
    });
  }

  // Row assignment, column by column from the left. Each node wants the average
  // row of the parents already placed to its left, and takes the first free row
  // at or below it.
  //
  // A tidy tree would centre each parent on its children instead, but the rank
  // graph is a DAG — `Vertex` is composed by `VertexConfig` *and* referenced by
  // `Edge` — and two parents' midpoints can round to the same row. Packing per
  // column makes rows unique by construction, and two columns are already more
  // than a node width apart, so nothing can overlap.
  function assignColumnSlots(members, edges, depths, startSlot) {
    const ids = new Set(
      members.map(function (node) {
        return node.id;
      }),
    );
    const parents = new Map();
    members.forEach(function (node) {
      parents.set(node.id, []);
    });
    edges.forEach(function (edge) {
      if (ids.has(edge.source) && ids.has(edge.target)) {
        parents.get(edge.source).push(edge.target);
      }
    });

    const byDepth = new Map();
    members.forEach(function (node) {
      const depth = depths.get(node.id) || 0;
      if (!byDepth.has(depth)) {
        byDepth.set(depth, []);
      }
      byDepth.get(depth).push(node);
    });

    const slots = new Map();
    let maxSlot = startSlot - 1;
    Array.from(byDepth.keys())
      .sort(function (a, b) {
        return a - b;
      })
      .forEach(function (depth) {
        const column = byDepth.get(depth).slice().sort(byLabel);
        const desired = new Map();
        column.forEach(function (node) {
          const placed = parents
            .get(node.id)
            .map(function (id) {
              return slots.get(id);
            })
            .filter(function (value) {
              return typeof value === "number";
            });
          desired.set(
            node.id,
            placed.length
              ? placed.reduce(function (a, b) {
                  return a + b;
                }, 0) / placed.length
              : startSlot,
          );
        });
        column.sort(function (a, b) {
          if (desired.get(a.id) !== desired.get(b.id)) {
            return desired.get(a.id) - desired.get(b.id);
          }
          return byLabel(a, b);
        });
        // Place the column as one contiguous block, centred on where its parents
        // are. Honouring each node's own desired row instead would leave a gap
        // wherever two nodes want the same one, and those gaps compound column by
        // column — the Ingestion band spanned ten rows for a six-node column.
        const mean =
          column.reduce(function (total, node) {
            return total + desired.get(node.id);
          }, 0) / column.length;
        const offset = Math.max(startSlot, Math.round(mean - (column.length - 1) / 2));
        column.forEach(function (node, index) {
          slots.set(node.id, offset + index);
          maxSlot = Math.max(maxSlot, offset + index);
        });
      });
    return { slots: slots, nextSlot: maxSlot + 1 };
  }

  // Pack the depths actually present into consecutive columns, per component,
  // so a shallow band reserves no empty columns for a deeper one.
  function localColumns(members, depths) {
    const present = Array.from(
      new Set(
        members.map(function (node) {
          return depths.get(node.id) || 0;
        }),
      ),
    ).sort(function (a, b) {
      return a - b;
    });
    const mapping = new Map();
    present.forEach(function (value, index) {
      mapping.set(value, index);
    });
    return mapping;
  }

  function layoutBand(band, taxonomyEdges, gridCols) {
    const edges = rankEdgesForBand(band.nodes, taxonomyEdges);
    const depths = assignDepths(band.nodes, edges, taxonomyEdges);
    const comps = componentsOf(band.nodes, edges);
    const trees = comps.filter(function (comp) {
      return comp.length > 1;
    });
    const singles = comps
      .filter(function (comp) {
        return comp.length === 1;
      })
      .map(function (comp) {
        return comp[0];
      })
      // Fallback only: a class that neither specialises nor composes anything in
      // its band. Enumerations used to land here; they now rank beside whatever
      // references them.
      .sort(function (a, b) {
        const aEnum = a.kind === "enum" ? 1 : 0;
        const bEnum = b.kind === "enum" ? 1 : 0;
        if (aEnum !== bEnum) {
          return aEnum - bEnum;
        }
        return byLabel(a, b);
      });

    trees.sort(function (a, b) {
      if (b.length !== a.length) {
        return b.length - a.length;
      }
      return byLabel(a[0], b[0]);
    });

    // Positions are band-relative here; packBands() translates them once the
    // band's place on the page is known.
    const contentY = LAYOUT.bandHeader + LAYOUT.bandPadY;
    let slot = 0;
    let right = data.nodeWidth;

    trees.forEach(function (members) {
      const columns = localColumns(members, depths);
      const assigned = assignColumnSlots(members, edges, depths, slot);
      members.forEach(function (node) {
        const col = columns.get(depths.get(node.id) || 0) || 0;
        node.bandX = LAYOUT.bandPadX + col * colStride;
        node.bandY = contentY + (assigned.slots.get(node.id) || 0) * rowStride;
        right = Math.max(right, node.bandX + data.nodeWidth);
      });
      slot = assigned.nextSlot;
    });

    singles.forEach(function (node, index) {
      const col = index % gridCols;
      const row = Math.floor(index / gridCols);
      node.bandX = LAYOUT.bandPadX + col * gridStride;
      node.bandY = contentY + (slot + row) * rowStride;
      right = Math.max(right, node.bandX + data.nodeWidth);
    });
    if (singles.length) {
      slot += Math.ceil(singles.length / gridCols);
    }

    return {
      id: band.id,
      label: band.label,
      nodes: band.nodes,
      x: 0,
      y: 0,
      width: right + LAYOUT.bandPadX,
      height:
        LAYOUT.bandHeader +
        LAYOUT.bandPadY * 2 +
        Math.max(slot * rowStride - LAYOUT.rowGap, data.nodeHeight),
    };
  }

  // Shelf-pack the bands left to right, wrapping past `targetWidth`. Stacking
  // them in one column wastes a third of the height on band chrome.
  function packBands(boxes, targetWidth) {
    let x = 0;
    let y = 0;
    let shelfHeight = 0;
    let width = 0;
    boxes.forEach(function (box) {
      if (x > 0 && x + box.width > targetWidth) {
        y += shelfHeight + LAYOUT.bandGap;
        x = 0;
        shelfHeight = 0;
      }
      box.x = x;
      box.y = y;
      x += box.width + LAYOUT.bandGap;
      shelfHeight = Math.max(shelfHeight, box.height);
      width = Math.max(width, x - LAYOUT.bandGap);
    });
    return { width: width, height: y + shelfHeight };
  }

  function layoutAll(gridCols) {
    const nodes = layoutNodes();
    const edges = layoutEdges();

    const byGroup = new Map();
    nodes.forEach(function (node) {
      const key = groupIndex.has(node.group) ? node.group : groups[0].id;
      if (!byGroup.has(key)) {
        byGroup.set(key, []);
      }
      byGroup.get(key).push(node);
    });

    const boxes = [];
    groups.forEach(function (group) {
      const members = byGroup.get(group.id);
      if (!members || !members.length) {
        return;
      }
      boxes.push(
        layoutBand({ id: group.id, label: group.label, nodes: members }, edges, gridCols),
      );
    });
    return boxes;
  }

  // A shelf break can only fall where some run of consecutive bands exactly
  // fills the width, so every contiguous run is a candidate target. Trying only
  // the prefixes — as this did — misses every arrangement whose second shelf is
  // wider than its first, which is most of the good ones.
  function shelfTargets(boxes) {
    const targets = new Set();
    for (let start = 0; start < boxes.length; start += 1) {
      let running = 0;
      for (let end = start; end < boxes.length; end += 1) {
        running += boxes[end].width + (end > start ? LAYOUT.bandGap : 0);
        targets.add(running);
      }
    }
    return Array.from(targets).sort(function (a, b) {
      return a - b;
    });
  }

  function fitScore(width, height) {
    if (!width || !height) {
      return 0;
    }
    return Math.min(LAYOUT.targetAspect / width, 1 / height);
  }

  function computeLayout() {
    // Search grid width x shelf width for the arrangement that fills a 16:9
    // frame best. A general rule, so no per-band geometry is written down.
    let best = null;
    LAYOUT.gridColsCandidates.forEach(function (cols) {
      const boxes = layoutAll(cols);
      shelfTargets(boxes).forEach(function (target) {
        const size = packBands(boxes, target);
        const score = fitScore(size.width, size.height);
        if (!best || score > best.score) {
          best = { score: score, cols: cols, target: target };
        }
      });
    });

    const boxes = layoutAll(best ? best.cols : 4);
    const size = packBands(boxes, best ? best.target : Infinity);
    // Drop coordinates of anything not in this layout, so a re-shown node can
    // never come back at a stale position.
    data.nodes.forEach(function (node) {
      if (nodeIsHidden(node)) {
        delete node.x;
        delete node.y;
      }
    });
    boxes.forEach(function (box) {
      box.nodes.forEach(function (node) {
        node.x = box.x + node.bandX;
        node.y = box.y + node.bandY;
      });
    });
    bandBoxes = boxes;
    data.bounds = { minX: 0, minY: 0, maxX: size.width, maxY: size.height };
  }

  function computeBounds(nodes) {
    if (!nodes.length) {
      return { minX: 0, minY: 0, maxX: data.nodeWidth, maxY: data.nodeHeight };
    }
    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;
    nodes.forEach(function (node) {
      minX = Math.min(minX, node.x);
      minY = Math.min(minY, node.y);
      maxX = Math.max(maxX, node.x + data.nodeWidth);
      maxY = Math.max(maxY, node.y + data.nodeHeight);
    });
    bandBoxes.forEach(function (box) {
      minX = Math.min(minX, box.x);
      minY = Math.min(minY, box.y);
      maxX = Math.max(maxX, box.x + box.width);
      maxY = Math.max(maxY, box.y + box.height);
    });
    return { minX: minX, minY: minY, maxX: maxX, maxY: maxY };
  }

  // ---------------------------------------------------------------- edges

  function nodeAnchor(node, toward) {
    const cy = node.y + data.nodeHeight / 2;
    const towardCx = toward.x + data.nodeWidth / 2;
    const cx = node.x + data.nodeWidth / 2;
    if (Math.abs(towardCx - cx) < data.nodeWidth / 2) {
      const towardCy = toward.y + data.nodeHeight / 2;
      return { x: cx, y: towardCy > cy ? node.y + data.nodeHeight : node.y, vertical: true };
    }
    return {
      x: towardCx > cx ? node.x + data.nodeWidth : node.x,
      y: cy,
      vertical: false,
    };
  }

  function edgePath(source, target) {
    const start = nodeAnchor(source, target);
    const end = nodeAnchor(target, source);
    if (start.vertical || end.vertical) {
      const my = (start.y + end.y) / 2;
      return (
        "M" + start.x + "," + start.y + " C" + start.x + "," + my + " " + end.x + "," + my + " " + end.x + "," + end.y
      );
    }
    const mx = (start.x + end.x) / 2;
    return (
      "M" + start.x + "," + start.y + " C" + mx + "," + start.y + " " + mx + "," + end.y + " " + end.x + "," + end.y
    );
  }

  function passesRelationMode(edge) {
    if (state.relationMode === "all") {
      return true;
    }
    if (state.relationMode === "taxonomy") {
      return edge.kind === "subClassOf" || edge.kind === "equivalentClass";
    }
    if (state.relationMode === "has") {
      return edge.label.toLowerCase().startsWith("has");
    }
    return true;
  }

  function edgeIsIncidentToSelected(edge, selectedId) {
    return edge.source === selectedId || edge.target === selectedId;
  }

  function edgeIsVisible(edge) {
    if (edgeIsSuppressed(edge)) {
      return false;
    }
    // A selected class always shows its own properties, whatever the filter says.
    if (state.selectedId && edgeIsIncidentToSelected(edge, state.selectedId)) {
      return true;
    }
    return passesRelationMode(edge);
  }

  function getVisibleEdges() {
    return data.edges.filter(edgeIsVisible);
  }

  // ---------------------------------------------------------------- render

  function clearLayer(className) {
    viewport.querySelectorAll("." + className).forEach(function (el) {
      el.remove();
    });
  }

  function renderBands() {
    clearLayer("band-layer");
    const layer = document.createElementNS(SVG_NS, "g");
    layer.setAttribute("class", "band-layer");
    bandBoxes.forEach(function (box) {
      const rect = document.createElementNS(SVG_NS, "rect");
      rect.setAttribute("class", "band");
      rect.setAttribute("x", box.x);
      rect.setAttribute("y", box.y);
      rect.setAttribute("width", box.width);
      rect.setAttribute("height", box.height);
      rect.setAttribute("rx", 12);
      rect.style.setProperty("--group-hue", groupHue(box.id));
      layer.appendChild(rect);

      const label = document.createElementNS(SVG_NS, "text");
      label.setAttribute("class", "band-label");
      label.setAttribute("x", box.x + LAYOUT.bandPadX);
      label.setAttribute("y", box.y + 18);
      label.style.setProperty("--group-hue", groupHue(box.id));
      label.textContent = box.label;
      layer.appendChild(label);
    });
    viewport.insertBefore(layer, viewport.firstChild);
  }

  function renderEdges(visibleEdges) {
    clearLayer("edge-layer");
    const layer = document.createElementNS(SVG_NS, "g");
    layer.setAttribute("class", "edge-layer");

    visibleEdges.forEach(function (edge) {
      const source = nodeById.get(edge.source);
      const target = nodeById.get(edge.target);
      if (!source || !target || nodeIsHidden(source) || nodeIsHidden(target)) {
        return;
      }

      const path = document.createElementNS(SVG_NS, "path");
      path.setAttribute("d", edgePath(source, target));
      path.setAttribute("class", "edge kind-" + edge.kind);
      path.setAttribute("marker-end", "url(#arrow-" + edge.kind + ")");
      path.dataset.edgeId = edge.id;
      layer.appendChild(path);

      if (edge.kind !== "subClassOf" && edge.kind !== "equivalentClass") {
        const label = document.createElementNS(SVG_NS, "text");
        label.setAttribute("class", "edge-label");
        label.setAttribute("x", (source.x + target.x + data.nodeWidth) / 2);
        label.setAttribute("y", (source.y + target.y + data.nodeHeight) / 2);
        label.setAttribute("text-anchor", "middle");
        label.dataset.edgeId = edge.id;
        label.textContent = edge.label;
        layer.appendChild(label);
      }
    });
    const bands = viewport.querySelector(".band-layer");
    viewport.insertBefore(layer, bands ? bands.nextSibling : viewport.firstChild);
  }

  function renderNodes() {
    clearLayer("node");
    layoutNodes().forEach(function (node) {
      const group = document.createElementNS(SVG_NS, "g");
      group.setAttribute("class", "node kind-" + node.kind);
      group.setAttribute("transform", "translate(" + node.x + "," + node.y + ")");
      group.dataset.nodeId = node.id;
      group.style.setProperty("--group-hue", groupHue(node.group));

      const rect = document.createElementNS(SVG_NS, "rect");
      rect.setAttribute("width", data.nodeWidth);
      rect.setAttribute("height", data.nodeHeight);
      rect.setAttribute("rx", 8);
      rect.setAttribute("ry", 8);
      group.appendChild(rect);

      const accent = document.createElementNS(SVG_NS, "rect");
      accent.setAttribute("class", "node-accent");
      accent.setAttribute("width", 5);
      accent.setAttribute("height", data.nodeHeight);
      accent.setAttribute("rx", 2.5);
      group.appendChild(accent);

      const text = document.createElementNS(SVG_NS, "text");
      text.setAttribute("x", data.nodeWidth / 2 + 3);
      text.setAttribute("y", data.nodeHeight / 2 + 4);
      text.setAttribute("text-anchor", "middle");
      text.textContent = truncate(node.label, 20);
      group.appendChild(text);

      const title = document.createElementNS(SVG_NS, "title");
      title.textContent = node.label;
      group.appendChild(title);

      viewport.appendChild(group);
    });
  }

  function reRenderGraph() {
    const visibleEdges = getVisibleEdges();
    data.bounds = computeBounds(layoutNodes());
    renderBands();
    renderNodes();
    renderEdges(visibleEdges);
    applyHighlight(visibleEdges);
  }

  function relayout() {
    computeLayout();
    reRenderGraph();
  }

  function applyTransform() {
    viewport.setAttribute(
      "transform",
      "translate(" + state.tx + "," + state.ty + ") scale(" + state.scale + ")",
    );
  }

  function fitToScreen() {
    const shell = document.querySelector(".graph-shell");
    const pad = 32;
    // The embed's toolbar floats over the canvas; keep the first band's label
    // out from under it.
    const toolbar = document.querySelector(".toolbar");
    const topPad = toolbar ? toolbar.getBoundingClientRect().height + 22 : pad;
    const bounds = data.bounds;
    const graphW = bounds.maxX - bounds.minX;
    const graphH = bounds.maxY - bounds.minY;
    const viewW = shell.clientWidth - pad * 2;
    const viewH = shell.clientHeight - topPad - pad;
    if (viewW <= 0 || viewH <= 0 || graphW <= 0 || graphH <= 0) {
      state.scale = 1;
      state.tx = pad - bounds.minX;
      state.ty = topPad - bounds.minY;
      applyTransform();
      return;
    }
    state.scale = Math.min(viewW / graphW, viewH / graphH, 1.3);
    state.tx = pad - bounds.minX * state.scale + (viewW - graphW * state.scale) / 2;
    state.ty = topPad - bounds.minY * state.scale + (viewH - graphH * state.scale) / 2;
    applyTransform();
  }

  function matchesSearch(node) {
    if (!state.search) {
      return true;
    }
    const q = state.search.toLowerCase();
    return node.local.toLowerCase().includes(q) || node.label.toLowerCase().includes(q);
  }

  function neighborhood(nodeId, visibleEdges) {
    const related = new Set([nodeId]);
    visibleEdges.forEach(function (edge) {
      if (edge.source === nodeId) {
        related.add(edge.target);
      }
      if (edge.target === nodeId) {
        related.add(edge.source);
      }
    });
    return related;
  }

  function applyHighlight(visibleEdges) {
    const edges = visibleEdges || getVisibleEdges();
    // One pass to index, instead of a linear scan per rendered edge element.
    const edgeById = new Map(
      edges.map(function (edge) {
        return [edge.id, edge];
      }),
    );
    const hasSelection = Boolean(state.selectedId);
    const hasSearch = Boolean(state.search);
    const focus = hasSelection ? neighborhood(state.selectedId, edges) : null;

    viewport.querySelectorAll(".node").forEach(function (group) {
      const nodeId = group.dataset.nodeId;
      const node = nodeById.get(nodeId);
      let dim = false;
      if (hasSearch && node && !matchesSearch(node)) {
        dim = true;
      }
      if (hasSelection && !focus.has(nodeId)) {
        dim = true;
      }
      group.classList.toggle("dimmed", dim);
      group.classList.toggle("selected", nodeId === state.selectedId);
    });

    viewport.querySelectorAll(".edge, .edge-label").forEach(function (element) {
      const edge = edgeById.get(element.dataset.edgeId);
      const dim =
        hasSelection && edge && !edgeIsIncidentToSelected(edge, state.selectedId);
      element.classList.toggle("dimmed", Boolean(dim));
    });
  }

  function selectNode(nodeId) {
    state.selectedId = state.selectedId === nodeId ? null : nodeId;
    updateDetails();
    reRenderGraph();
  }

  // ---------------------------------------------------------------- details

  function parentsOf(nodeId) {
    return allSubclassEdges
      .filter(function (edge) {
        return edge.source === nodeId;
      })
      .map(function (edge) {
        return nodeById.get(edge.target);
      })
      .filter(Boolean);
  }

  function subclassesOf(nodeId) {
    return allSubclassEdges
      .filter(function (edge) {
        return edge.target === nodeId;
      })
      .map(function (edge) {
        return nodeById.get(edge.source);
      })
      .filter(Boolean);
  }

  // Longest path to a root over the *full* taxonomy, so the suppressed universal
  // root still shows up here.
  function ancestryPath(nodeId) {
    const seen = new Set();
    function walk(id) {
      if (seen.has(id)) {
        return [];
      }
      seen.add(id);
      let best = [];
      parentsOf(id).forEach(function (parent) {
        const candidate = walk(parent.id);
        if (candidate.length > best.length) {
          best = candidate;
        }
      });
      seen.delete(id);
      const node = nodeById.get(id);
      return best.concat(node ? [node] : []);
    }
    return walk(nodeId);
  }

  function escapeHtml(text) {
    return String(text)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function groupLabel(groupId) {
    const found = groups.find(function (group) {
      return group.id === groupId;
    });
    return found ? found.label : groupId;
  }

  function updateDetails() {
    const panel = document.getElementById("details");
    if (!panel) {
      return;
    }
    if (!state.selectedId) {
      panel.innerHTML = "<p>Select a class to inspect its IRI and description.</p>";
      return;
    }
    const node = nodeById.get(state.selectedId);
    if (!node) {
      return;
    }
    const props = data.edges.filter(function (edge) {
      return edge.kind === "objectProperty" && (edge.source === node.id || edge.target === node.id);
    });
    const path = ancestryPath(node.id);

    let html = "<h2>" + escapeHtml(node.label) + "</h2>";
    html += "<div class='uri'><code>" + escapeHtml(node.id) + "</code></div>";
    if (path.length > 1) {
      html +=
        "<div class='breadcrumb'>" +
        path
          .map(function (item) {
            return escapeHtml(item.local);
          })
          .join(" › ") +
        "</div>";
    }
    html += "<p><strong>Block:</strong> " + escapeHtml(groupLabel(node.group)) + "</p>";
    if (node.comment) {
      html += "<div class='comment'>" + escapeHtml(node.comment) + "</div>";
    }

    const parents = parentsOf(node.id);
    if (parents.length) {
      html +=
        "<p><strong>Parents:</strong> " +
        parents
          .map(function (item) {
            return escapeHtml(item.local);
          })
          .join(", ") +
        "</p>";
    }
    const subclasses = subclassesOf(node.id);
    if (subclasses.length) {
      html +=
        "<p><strong>Subclasses:</strong> " +
        subclasses
          .map(function (item) {
            return escapeHtml(item.local);
          })
          .join(", ") +
        "</p>";
    }
    if (node.enumValues && node.enumValues.length) {
      html += "<p><strong>Values:</strong></p><ul class='values'>";
      node.enumValues.forEach(function (item) {
        html +=
          "<li><code>" + escapeHtml(item.value) + "</code> " + escapeHtml(item.label) + "</li>";
      });
      html += "</ul>";
    }
    if (node.datatypeProperties && node.datatypeProperties.length) {
      html += "<p><strong>Attributes:</strong></p><ul class='attrs'>";
      node.datatypeProperties.forEach(function (item) {
        html +=
          "<li><code>" +
          escapeHtml(item.label) +
          "</code><span class='range'>" +
          escapeHtml(item.range) +
          "</span></li>";
      });
      html += "</ul>";
    }
    if (props.length) {
      html += "<p><strong>Relations:</strong></p><ul>";
      props.slice(0, 12).forEach(function (edge) {
        html += "<li><code>" + escapeHtml(edge.label) + "</code></li>";
      });
      if (props.length > 12) {
        html += "<li>… +" + (props.length - 12) + " more</li>";
      }
      html += "</ul>";
    }
    panel.innerHTML = html;
  }

  // ---------------------------------------------------------------- chrome

  function swatch(styleValue) {
    return "<span class='swatch' style='" + styleValue + "'></span> ";
  }

  function renderLegend() {
    const legend = document.getElementById("legend");
    if (!legend) {
      return;
    }
    let html = "<div class='legend-title'>Blocks</div>";
    groups.forEach(function (group) {
      html +=
        "<div class='legend-item'>" +
        swatch("background:hsl(" + groupHue(group.id) + " 55% 45%)") +
        escapeHtml(group.label) +
        "</div>";
    });
    html += "<div class='legend-title'>Classes</div>";
    html += "<div class='legend-item'>" + swatch("background:var(--gf-node)") + "class</div>";
    html +=
      "<div class='legend-item'>" +
      swatch("background:var(--enum-node)") +
      "enumeration</div>";
    html +=
      "<div class='legend-item'>" +
      swatch("background:var(--external-node)") +
      "external (prov, …)</div>";
    html += "<div class='legend-title'>Relations</div>";
    html +=
      "<div class='legend-item'>" + swatch("background:var(--edge-subclass)") + "subClassOf</div>";
    html +=
      "<div class='legend-item'>" +
      swatch("background:var(--edge-object)") +
      "object property</div>";
    legend.innerHTML = html;
  }

  function renderSharedProperties() {
    const host = document.getElementById("shared-properties");
    if (!host) {
      return;
    }
    const shared = data.sharedProperties || [];
    if (!shared.length) {
      host.remove();
      return;
    }
    let html =
      "<summary>Shared attributes (" + shared.length + ")</summary><ul class='attrs'>";
    shared.forEach(function (item) {
      html +=
        "<li><code>" +
        escapeHtml(item.label) +
        "</code><span class='range'>" +
        escapeHtml(item.range) +
        "</span></li>";
    });
    html += "</ul>";
    host.innerHTML = html;
  }

  function setupUniversalRootToggle() {
    const wrapper = document.getElementById("root-toggle");
    const checkbox = document.getElementById("show-universal-root");
    if (!wrapper || !checkbox) {
      return;
    }
    if (!rootNode) {
      wrapper.remove();
      return;
    }
    const label = document.getElementById("root-toggle-label");
    if (label) {
      label.textContent = "Show " + rootNode.local;
    }
    const note = document.getElementById("root-note");
    if (note) {
      note.textContent = "Every class is a " + rootNode.local + "; those links are hidden by default.";
    }
    checkbox.checked = state.showUniversalRoot;
    checkbox.addEventListener("change", function (event) {
      state.showUniversalRoot = event.target.checked;
      if (state.selectedId === data.universalRoot && !state.showUniversalRoot) {
        state.selectedId = null;
        updateDetails();
      }
      relayout();
      fitToScreen();
    });
  }

  function applyScheme(scheme) {
    if (scheme === "slate" || scheme === "dark") {
      document.documentElement.setAttribute("data-scheme", "slate");
    } else if (scheme === "default" || scheme === "light") {
      document.documentElement.setAttribute("data-scheme", "default");
    }
  }

  function setupTheme() {
    const params = new URLSearchParams(window.location.search);
    applyScheme(params.get("scheme"));
    window.addEventListener("message", function (event) {
      if (event.data && typeof event.data.grafloScheme === "string") {
        applyScheme(event.data.grafloScheme);
      }
    });
  }

  function nodeFromEventTarget(target) {
    const group = target.closest ? target.closest(".node") : null;
    if (!group) {
      return null;
    }
    return nodeById.get(group.dataset.nodeId) || null;
  }

  function bindControls() {
    const search = document.getElementById("search");
    const relationFilter = document.getElementById("relation-filter");
    if (search) {
      search.addEventListener("input", function (event) {
        state.search = event.target.value.trim();
        applyHighlight();
      });
    }
    if (relationFilter) {
      relationFilter.value = state.relationMode;
      relationFilter.addEventListener("change", function (event) {
        state.relationMode = event.target.value;
        reRenderGraph();
      });
    }

    document.getElementById("fit-button").addEventListener("click", fitToScreen);
    document.getElementById("reset-button").addEventListener("click", function () {
      state.selectedId = null;
      state.search = "";
      state.relationMode = "taxonomy";
      state.showUniversalRoot = false;
      if (search) {
        search.value = "";
      }
      if (relationFilter) {
        relationFilter.value = "taxonomy";
      }
      const checkbox = document.getElementById("show-universal-root");
      if (checkbox) {
        checkbox.checked = false;
      }
      updateDetails();
      relayout();
      fitToScreen();
    });

    svg.addEventListener("wheel", function (event) {
      event.preventDefault();
      const delta = event.deltaY > 0 ? 0.92 : 1.08;
      const rect = svg.getBoundingClientRect();
      const px = event.clientX - rect.left;
      const py = event.clientY - rect.top;
      state.tx = px - (px - state.tx) * delta;
      state.ty = py - (py - state.ty) * delta;
      state.scale *= delta;
      applyTransform();
    }, { passive: false });

    svg.addEventListener("mousedown", function (event) {
      state.dragMoved = false;
      state.pointerDownX = event.clientX;
      state.pointerDownY = event.clientY;
      const node = nodeFromEventTarget(event.target);
      state.lastX = event.clientX;
      state.lastY = event.clientY;
      if (node) {
        state.draggingNodeId = node.id;
        svg.classList.add("dragging-node");
        return;
      }
      state.draggingViewport = true;
      svg.classList.add("dragging");
    });

    window.addEventListener("mouseup", function () {
      const pendingNodeId = state.draggingNodeId;
      if (pendingNodeId && !state.dragMoved) {
        selectNode(pendingNodeId);
      }
      state.draggingNodeId = null;
      state.draggingViewport = false;
      svg.classList.remove("dragging");
      svg.classList.remove("dragging-node");
    });

    window.addEventListener("mousemove", function (event) {
      markDragIfNeeded(event);
      const dx = event.clientX - state.lastX;
      const dy = event.clientY - state.lastY;
      state.lastX = event.clientX;
      state.lastY = event.clientY;

      if (state.draggingNodeId) {
        if (!state.dragMoved) {
          return;
        }
        const node = nodeById.get(state.draggingNodeId);
        if (!node) {
          return;
        }
        node.x += dx / state.scale;
        node.y += dy / state.scale;
        reRenderGraph();
        return;
      }

      if (!state.draggingViewport) {
        return;
      }
      state.tx += dx;
      state.ty += dy;
      applyTransform();
    });

    svg.addEventListener("click", function (event) {
      const wasDrag = state.dragMoved;
      state.dragMoved = false;
      if (wasDrag) {
        return;
      }
      if (nodeFromEventTarget(event.target)) {
        return;
      }
      if (state.selectedId) {
        state.selectedId = null;
        updateDetails();
        reRenderGraph();
      }
    });
  }

  setupTheme();
  renderLegend();
  renderSharedProperties();
  setupUniversalRootToggle();
  relayout();
  bindControls();
  updateDetails();
  fitToScreen();
  window.addEventListener("resize", fitToScreen);
})();
