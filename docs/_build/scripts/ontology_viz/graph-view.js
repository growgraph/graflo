(function () {
  "use strict";

  const data = window.GRAFLO_ONTOLOGY_GRAPH;
  if (!data) {
    return;
  }

  const LAYOUT = {
    isoPad: 20,
    isoGap: 14,
    hierarchyGapX: 100,
    vGap: 118,
    linkDistance: 100,
    maxTicks: 420,
    alphaMin: 0.001,
    minGapX: 24,
    minGapY: 20,
    resolvePasses: 70,
  };

  // Ignore sub-pixel jitter so tap/click can select nodes instead of being treated as drag.
  const DRAG_THRESHOLD_PX = 5;

  const svg = document.getElementById("graph");
  const viewport = document.createElementNS("http://www.w3.org/2000/svg", "g");
  svg.appendChild(viewport);

  const defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
  [
    ["subClassOf", "#546e7a", 8],
    ["subClassOfReverse", "#7b5ea7", 7],
    ["equivalentClass", "#009688", 7],
    ["objectProperty", "#1565c0", 6],
    ["datatypeProperty", "#6d4c41", 6],
  ].forEach(function (entry) {
    const kind = entry[0];
    const color = entry[1];
    const size = entry[2];
    const marker = document.createElementNS("http://www.w3.org/2000/svg", "marker");
    marker.setAttribute("id", "arrow-" + kind);
    marker.setAttribute("viewBox", "0 -4 8 8");
    marker.setAttribute("refX", 7);
    marker.setAttribute("refY", 0);
    marker.setAttribute("markerWidth", size);
    marker.setAttribute("markerHeight", size);
    marker.setAttribute("orient", "auto");
    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("d", "M0,-4L8,0L0,4");
    path.setAttribute("fill", color);
    marker.appendChild(path);
    defs.appendChild(marker);
  });
  svg.insertBefore(defs, viewport);

  const nodeById = new Map(
    data.nodes.map(function (node) {
      return [node.id, node];
    }),
  );
  const subclassEdges = data.edges.filter(function (edge) {
    return edge.kind === "subClassOf";
  });

  const state = {
    scale: 1,
    tx: 40,
    ty: 40,
    selectedId: null,
    search: "",
    relationMode: "all",
    draggingViewport: false,
    draggingNodeId: null,
    dragMoved: false,
    pointerDownX: 0,
    pointerDownY: 0,
    lastX: 0,
    lastY: 0,
  };

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

  function isOverlapping(a, b) {
    const minDx = data.nodeWidth + LAYOUT.minGapX;
    const minDy = data.nodeHeight + LAYOUT.minGapY;
    return Math.abs(a.x - b.x) < minDx && Math.abs(a.y - b.y) < minDy;
  }

  function resolveOverlaps(nodes) {
    for (let pass = 0; pass < LAYOUT.resolvePasses; pass += 1) {
      let moved = false;
      for (let i = 0; i < nodes.length; i += 1) {
        for (let j = i + 1; j < nodes.length; j += 1) {
          const a = nodes[i];
          const b = nodes[j];
          if (!isOverlapping(a, b)) {
            continue;
          }

          const minDx = data.nodeWidth + LAYOUT.minGapX;
          const minDy = data.nodeHeight + LAYOUT.minGapY;
          const dx = (b.x - a.x) || 0.1;
          const dy = (b.y - a.y) || 0.1;
          const overlapX = minDx - Math.abs(dx);
          const overlapY = minDy - Math.abs(dy);
          if (overlapX <= 0 || overlapY <= 0) {
            continue;
          }

          if (overlapX < overlapY) {
            const push = overlapX / 2;
            const dir = dx >= 0 ? 1 : -1;
            a.x -= push * dir;
            b.x += push * dir;
          } else {
            const push = overlapY / 2;
            const dir = dy >= 0 ? 1 : -1;
            a.y -= push * dir;
            b.y += push * dir;
          }
          moved = true;
        }
      }
      if (!moved) {
        break;
      }
    }
  }

  function computeLevels(nodeIds, edges) {
    const children = new Map();
    nodeIds.forEach(function (id) {
      children.set(id, []);
    });
    edges.forEach(function (edge) {
      if (!children.has(edge.target)) {
        children.set(edge.target, []);
      }
      children.get(edge.target).push(edge.source);
    });

    const isChild = new Set(edges.map(function (edge) {
      return edge.source;
    }));
    const roots = nodeIds.filter(function (id) {
      return !isChild.has(id);
    });

    const level = new Map();
    const queue = roots.map(function (id) {
      return [id, 0];
    });
    while (queue.length) {
      const item = queue.shift();
      const id = item[0];
      const lv = item[1];
      if (level.has(id)) {
        continue;
      }
      level.set(id, lv);
      (children.get(id) || []).forEach(function (child) {
        queue.push([child, lv + 1]);
      });
    }
    nodeIds.forEach(function (id) {
      if (!level.has(id)) {
        level.set(id, 0);
      }
    });
    return level;
  }

  function placeIsolatedNodes(isolated, offsetX, offsetY) {
    isolated.forEach(function (node, index) {
      node.layoutGroup = "isolated";
      node.x = offsetX;
      node.y = offsetY + index * (data.nodeHeight + LAYOUT.isoGap);
    });
    if (!isolated.length) {
      return { width: 0, height: 0 };
    }
    return {
      width: data.nodeWidth,
      height: isolated.length * (data.nodeHeight + LAYOUT.isoGap) - LAYOUT.isoGap,
    };
  }

  function runSubclassForceLayout(hierarchyNodes, edges, originX, originY, levels) {
    const links = edges
      .map(function (edge) {
        return {
          source: nodeById.get(edge.source),
          target: nodeById.get(edge.target),
        };
      })
      .filter(function (link) {
        return link.source && link.target;
      });

    hierarchyNodes.forEach(function (node, index) {
      node.layoutGroup = "hierarchy";
      node.level = levels.get(node.id) || 0;
  node.x = originX + (index % 2) * 36 + (Math.random() - 0.5) * 4;
      node.y = originY + node.level * LAYOUT.vGap;
      node.vx = 0;
      node.vy = 0;
    });

    let alpha = 1;
    const centerX = originX + data.nodeWidth * 0.4;
    for (let tick = 0; tick < LAYOUT.maxTicks && alpha > LAYOUT.alphaMin; tick += 1) {
      hierarchyNodes.forEach(function (node) {
        node.vx = 0;
        node.vy = 0;
      });

      for (let i = 0; i < hierarchyNodes.length; i += 1) {
        for (let j = i + 1; j < hierarchyNodes.length; j += 1) {
          const a = hierarchyNodes[i];
          const b = hierarchyNodes[j];
          let dx = b.x - a.x;
          let dy = b.y - a.y;
          const dist = Math.sqrt(dx * dx + dy * dy) || 1;
          const repulse = (620 * alpha) / dist;
          dx = (dx / dist) * repulse;
          dy = (dy / dist) * repulse;
          a.vx -= dx;
          a.vy -= dy;
          b.vx += dx;
          b.vy += dy;
        }
      }

      links.forEach(function (link) {
        let dx = link.target.x - link.source.x;
        let dy = link.target.y - link.source.y;
        const dist = Math.sqrt(dx * dx + dy * dy) || 1;
        const strength = 0.55 * alpha;
        const delta = ((dist - LAYOUT.linkDistance) / dist) * strength;
        dx *= delta;
        dy *= delta;
        link.source.vx += dx;
        link.source.vy += dy;
        link.target.vx -= dx;
        link.target.vy -= dy;
      });

      hierarchyNodes.forEach(function (node) {
        const targetY = originY + node.level * LAYOUT.vGap;
        node.vy += (targetY - node.y) * 0.42 * alpha;
        node.vx += (centerX - node.x) * 0.08 * alpha;
      });

      hierarchyNodes.forEach(function (node) {
        node.x += node.vx * 0.18;
        node.y += node.vy * 0.18;
      });

      alpha *= 0.965
    }
  }

  function computeLayout() {
    const hierarchyIds = new Set();
    subclassEdges.forEach(function (edge) {
      hierarchyIds.add(edge.source);
      hierarchyIds.add(edge.target);
    });

    const isolated = data.nodes.filter(function (node) {
      return !hierarchyIds.has(node.id);
    });
    const hierarchy = data.nodes.filter(function (node) {
      return hierarchyIds.has(node.id);
    });

    const isoBox = placeIsolatedNodes(isolated, LAYOUT.isoPad, LAYOUT.isoPad);
    const hierarchyOriginX = LAYOUT.isoPad + isoBox.width + LAYOUT.hierarchyGapX;
    const hierarchyOriginY = LAYOUT.isoPad;
    const levels = computeLevels(
      hierarchy.map(function (node) {
        return node.id;
      }),
      subclassEdges,
    );
    runSubclassForceLayout(hierarchy, subclassEdges, hierarchyOriginX, hierarchyOriginY, levels);
    resolveOverlaps(hierarchy);
    resolveOverlaps(isolated);
    resolveOverlaps(data.nodes);
    data.bounds = computeBounds(data.nodes);
  }

  function computeBounds(nodes) {
    const xs = nodes.map(function (node) {
      return node.x;
    });
    const ys = nodes.map(function (node) {
      return node.y;
    });
    if (!xs.length) {
      return { minX: 0, minY: 0, maxX: data.nodeWidth, maxY: data.nodeHeight };
    }
    return {
      minX: Math.min.apply(null, xs),
      minY: Math.min.apply(null, ys),
      maxX: Math.max.apply(null, xs) + data.nodeWidth,
      maxY: Math.max.apply(null, ys) + data.nodeHeight,
    };
  }

  function nodeAnchor(node, toward) {
    const cx = node.x + data.nodeWidth / 2;
    const cy = node.y + data.nodeHeight / 2;
    const tx = toward.x + data.nodeWidth / 2;
    const ty = toward.y + data.nodeHeight / 2;
    const dx = tx - cx;
    const dy = ty - cy;
    if (!dx && !dy) {
      return { x: cx, y: cy };
    }
    const hw = data.nodeWidth / 2;
    const hh = data.nodeHeight / 2;
    const scale = Math.min(hw / (Math.abs(dx) || 1e-6), hh / (Math.abs(dy) || 1e-6));
    return { x: cx + dx * scale, y: cy + dy * scale };
  }

  function edgePath(source, target) {
    const start = nodeAnchor(source, target);
    const end = nodeAnchor(target, source);
    const mx = (start.x + end.x) / 2;
    const my = (start.y + end.y) / 2;
    return "M" + start.x + "," + start.y + " Q" + mx + "," + my + " " + end.x + "," + end.y;
  }

  function edgeVisibleByMode(edge) {
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

  function getVisibleEdges() {
    return data.edges.filter(edgeVisibleByMode);
  }

  function renderEdges() {
    viewport.querySelectorAll(".edge-layer").forEach(function (el) {
      el.remove();
    });
    const layer = document.createElementNS("http://www.w3.org/2000/svg", "g");
    layer.setAttribute("class", "edge-layer");

    getVisibleEdges().forEach(function (edge) {
      const source = nodeById.get(edge.source);
      const target = nodeById.get(edge.target);
      if (!source || !target) {
        return;
      }

      const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
      path.setAttribute("d", edgePath(source, target));
      path.setAttribute("class", "edge kind-" + edge.kind);
      path.setAttribute("marker-end", "url(#arrow-" + edge.kind + ")");
      path.dataset.edgeId = edge.id;
      layer.appendChild(path);

      if (edge.kind === "subClassOf") {
        const reverse = document.createElementNS("http://www.w3.org/2000/svg", "path");
        reverse.setAttribute("d", edgePath(target, source));
        reverse.setAttribute("class", "edge edge-reverse kind-subClassOfReverse");
        reverse.setAttribute("marker-end", "url(#arrow-subClassOfReverse)");
        reverse.dataset.edgeId = edge.id + ":reverse";
        layer.appendChild(reverse);
      } else if (edge.kind === "equivalentClass") {
        const reverseEq = document.createElementNS("http://www.w3.org/2000/svg", "path");
        reverseEq.setAttribute("d", edgePath(target, source));
        reverseEq.setAttribute("class", "edge kind-equivalentClass");
        reverseEq.setAttribute("marker-end", "url(#arrow-equivalentClass)");
        reverseEq.dataset.edgeId = edge.id + ":reverse";
        layer.appendChild(reverseEq);
      } else {
        const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
        label.setAttribute("class", "edge-label");
        label.setAttribute("x", (source.x + target.x + data.nodeWidth) / 2);
        label.setAttribute("y", (source.y + target.y + data.nodeHeight) / 2);
        label.setAttribute("text-anchor", "middle");
        label.dataset.edgeId = edge.id;
        label.textContent = edge.label;
        layer.appendChild(label);
      }
    });
    viewport.insertBefore(layer, viewport.firstChild);
  }

  function renderNodes() {
    viewport.querySelectorAll(".node").forEach(function (el) {
      el.remove();
    });
    data.nodes.forEach(function (node) {
      const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
      group.setAttribute("class", "node kind-" + node.kind + " group-" + node.layoutGroup);
      group.setAttribute("transform", "translate(" + node.x + "," + node.y + ")");
      group.dataset.nodeId = node.id;

      const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      rect.setAttribute("width", data.nodeWidth);
      rect.setAttribute("height", data.nodeHeight);
      rect.setAttribute("rx", 8);
      rect.setAttribute("ry", 8);
      group.appendChild(rect);

      const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
      text.setAttribute("x", data.nodeWidth / 2);
      text.setAttribute("y", data.nodeHeight / 2 + 4);
      text.setAttribute("text-anchor", "middle");
      text.textContent = truncate(node.label, 18);
      group.appendChild(text);

      const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
      title.textContent = node.label;
      group.appendChild(title);

      viewport.appendChild(group);
    });
  }

  function reRenderGraph() {
    data.bounds = computeBounds(data.nodes);
    renderNodes();
    renderEdges();
    applyHighlight();
  }

  function applyTransform() {
    viewport.setAttribute(
      "transform",
      "translate(" + state.tx + "," + state.ty + ") scale(" + state.scale + ")",
    );
  }

  function fitToScreen() {
    const shell = document.querySelector(".graph-shell");
    const pad = 48;
    const bounds = data.bounds;
    const graphW = bounds.maxX - bounds.minX;
    const graphH = bounds.maxY - bounds.minY;
    const viewW = shell.clientWidth - pad * 2;
    const viewH = shell.clientHeight - pad * 2;
    if (viewW <= 0 || viewH <= 0 || graphW <= 0 || graphH <= 0) {
      state.scale = 1;
      state.tx = pad - bounds.minX;
      state.ty = pad - bounds.minY;
      applyTransform();
      return;
    }
    state.scale = Math.min(viewW / graphW, viewH / graphH, 1.3);
    state.tx = pad - bounds.minX * state.scale + (viewW - graphW * state.scale) / 2;
    state.ty = pad - bounds.minY * state.scale + (viewH - graphH * state.scale) / 2;
    applyTransform();
  }

  function matchesSearch(node) {
    if (!state.search) {
      return true;
    }
    const q = state.search.toLowerCase();
    return node.local.toLowerCase().includes(q) || node.label.toLowerCase().includes(q);
  }

  function neighborhood(nodeId) {
    const related = new Set([nodeId]);
    getVisibleEdges().forEach(function (edge) {
      if (edge.source === nodeId) {
        related.add(edge.target);
      }
      if (edge.target === nodeId) {
        related.add(edge.source);
      }
    });
    return related;
  }

  function edgeIsIncidentToSelected(edge, selectedId) {
    return edge.source === selectedId || edge.target === selectedId;
  }

  function findVisibleEdge(edgeId) {
    return getVisibleEdges().find(function (item) {
      return item.id === edgeId;
    });
  }

  function applyHighlight() {
    const hasSelection = Boolean(state.selectedId);
    const hasSearch = Boolean(state.search);
    const focus = hasSelection ? neighborhood(state.selectedId) : null;

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

    viewport.querySelectorAll(".edge").forEach(function (edgeEl) {
      const edgeId = edgeEl.dataset.edgeId.replace(/:reverse$/, "");
      const edge = findVisibleEdge(edgeId);
      let dim = false;
      if (hasSelection && edge && !edgeIsIncidentToSelected(edge, state.selectedId)) {
        dim = true;
      }
      edgeEl.classList.toggle("dimmed", dim);
    });

    viewport.querySelectorAll(".edge-label").forEach(function (labelEl) {
      const edge = findVisibleEdge(labelEl.dataset.edgeId);
      let dim = false;
      if (hasSelection && edge && !edgeIsIncidentToSelected(edge, state.selectedId)) {
        dim = true;
      }
      labelEl.classList.toggle("dimmed", dim);
    });
  }

  function selectNode(nodeId) {
    state.selectedId = state.selectedId === nodeId ? null : nodeId;
    updateDetails();
    applyHighlight();
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
      return edge.kind !== "subClassOf" && (edge.source === node.id || edge.target === node.id);
    });
    const subclasses = data.edges
      .filter(function (edge) {
        return edge.kind === "subClassOf" && edge.target === node.id;
      })
      .map(function (edge) {
        return nodeById.get(edge.source);
      })
      .filter(Boolean);
    const parents = data.edges
      .filter(function (edge) {
        return edge.kind === "subClassOf" && edge.source === node.id;
      })
      .map(function (edge) {
        return nodeById.get(edge.target);
      })
      .filter(Boolean);

    let html = "<h2>" + node.label + "</h2>";
    html += "<div class='uri'><code>" + node.id + "</code></div>";
    if (node.comment) {
      html += "<div class='comment'>" + node.comment + "</div>";
    }
    if (node.layoutGroup === "isolated") {
      html += "<p><strong>Layout:</strong> standalone class (no <code>subClassOf</code> links)</p>";
    }
    if (parents.length) {
      html += "<p><strong>Parents:</strong> " + parents.map(function (p) {
        return p.local;
      }).join(", ") + "</p>";
    }
    if (subclasses.length) {
      html += "<p><strong>Subclasses:</strong> " + subclasses.map(function (p) {
        return p.local;
      }).join(", ") + "</p>";
    }
    if (props.length) {
      html += "<p><strong>Properties:</strong></p><ul>";
      props.slice(0, 12).forEach(function (edge) {
        html += "<li><code>" + edge.label + "</code></li>";
      });
      if (props.length > 12) {
        html += "<li>… +" + (props.length - 12) + " more</li>";
      }
      html += "</ul>";
    }
    panel.innerHTML = html;
  }

  function nodeFromEventTarget(target) {
    const group = target.closest ? target.closest(".node") : null;
    if (!group) {
      return null;
    }
    const nodeId = group.dataset.nodeId;
    return nodeById.get(nodeId) || null;
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
      relationFilter.addEventListener("change", function (event) {
        state.relationMode = event.target.value;
        renderEdges();
        applyHighlight();
      });
    }

    document.getElementById("fit-button").addEventListener("click", fitToScreen);
    document.getElementById("reset-button").addEventListener("click", function () {
      state.selectedId = null;
      state.search = "";
      if (search) {
        search.value = "";
      }
      if (relationFilter) {
        relationFilter.value = "all";
      }
      state.relationMode = "all";
      updateDetails();
      renderEdges();
      applyHighlight();
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

    window.addEventListener("mouseup", function (event) {
      const pendingNodeId = state.draggingNodeId;
      const draggedNode = pendingNodeId ? nodeById.get(pendingNodeId) : null;
      if (draggedNode && state.dragMoved) {
        resolveOverlaps(data.nodes);
        reRenderGraph();
      } else if (pendingNodeId && !state.dragMoved) {
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
      markDragIfNeeded(event);
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
        applyHighlight();
      }
    });
  }

  computeLayout();
  reRenderGraph();
  bindControls();
  updateDetails();
  fitToScreen();
  window.addEventListener("resize", fitToScreen);
})();
