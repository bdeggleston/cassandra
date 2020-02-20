
let fg_ids = [];

let fg_zoomed = {};
let fg_viewBoxes = {};

function fg_cell_mouseover(group, output, detail) {
    debugger;
    const cell = group.querySelector('rect');
    const text = group.querySelector('text');
    cell.setAttribute('stroke', 'blue');
    // text.setAttribute('__text', text.textContent);
    // text.textContent = text.getAttribute('fg-text');
    output.textContent = group.getAttribute("${FG_OUTPUT_ATTR}");
    let detail_txt = group.getAttribute("${FG_PARENT_PERCENT_ATTR}") + "% of parent";
    let parent_diff = group.getAttribute("${FG_PARENT_DIFF_ATTR}");
    if (parent_diff.length > 0)
        detail_txt = detail_txt + " (" + parent_diff + "% change)";

    detail_txt = detail_txt + ", " + group.getAttribute("${FG_TOTAL_PERCENT_ATTR}") + "% of total";

    let total_diff = group.getAttribute("${FG_TOTAL_DIFF_ATTR}");
    if (total_diff.length > 0)
        detail_txt = detail_txt + " (" + total_diff + "% change)";


    detail_txt = detail_txt + ", " + group.getAttribute("${FG_SAMPLES_ATTR}") + " samples";

    detail.textContent = detail_txt;
}

function fg_cell_mouseout(group) {
    const cell = group.querySelector('rect');
    const text = group.querySelector('text');
    cell.removeAttribute('stroke');
    // text.textContent = text.getAttribute('__text');
    // text.removeAttribute('__text');
}

function _store_attr(obj, param) {
    const store_name = '__' + param;
    const current = obj.getAttribute(param);
    obj.setAttribute(store_name, current);
}

function _restore_attr(obj, param) {
    const store_name = '__' + param;
    const prev = obj.getAttribute(store_name);
    obj.setAttribute(param, prev);
    obj.removeAttribute(store_name);
}

function _store_pos(obj) {
    _store_attr(obj, 'x');
    _store_attr(obj, 'y');
    _store_attr(obj, 'width');
    _store_attr(obj, 'height');
}

function _restore_pos(obj) {
    _restore_attr(obj, 'x');
    _restore_attr(obj, 'y');
    _restore_attr(obj, 'width');
    _restore_attr(obj, 'height');
}

function fg_trim_text(graph) {
    graph.querySelectorAll("g").forEach((group) => {
        if (group.hasAttribute("fg-hidden"))
            return;

        const cell = group.querySelector('rect');
        const text = group.querySelector('text');
        const cell_width = parseFloat(cell.getAttribute('width'));

        if (cell_width < 20) {
            text.textContent = "";
            return;
        }

        const text_contents = group.getAttribute('${FG_TEXT_ATTR}');
        text.textContent = text_contents;
        const text_width = text.getComputedTextLength();
        if (cell_width > text_width)
            return;

        let char_width = text_width / text.textContent.length;

        let max_chars = parseInt(cell_width / char_width);

        if (max_chars < 6)
        {
            text.textContent = "";
            return;
        }

        text.textContent = text_contents.substr(0, max_chars - 4) + '...';
    });
}

function fg_zoom(graph, target) {
    if (fg_zoomed[graph])
        fg_unzoom(graph, false);

    const t_cell = target.querySelector('rect');
    let graph_width = parseFloat(graph.getAttribute('fg-vb-width'));
    let graph_height = parseFloat(graph.getAttribute('fg-vb-height'));
    let cell_width = parseFloat(t_cell.getAttribute('width'));
    let cell_height = parseFloat(t_cell.getAttribute('height'));

    const min_x = parseFloat(t_cell.getAttribute('x'));
    const max_x = min_x + parseFloat(t_cell.getAttribute('width'));

    const max_y = parseFloat(t_cell.getAttribute('y'));
    // const max_y = min_y + parseFloat(t_cell.getAttribute('width'));
    const scale = graph_width / cell_width;
    const min_level = parseInt(t_cell.getAttribute('${FG_LEVEL_ATTR}'));

    graph.querySelectorAll("g").forEach((group) => {
        const cell = group.querySelector('rect');
        const text = group.querySelector('text');

        const x = parseFloat(cell.getAttribute('x'));
        const y = parseFloat(cell.getAttribute('y'));

        let keep = !(x < min_x || x > max_x || y > max_y);

        if (!keep) {
            group.setAttribute("fg-hidden", "1");
            cell.setAttribute('visibility', 'hidden');
            text.setAttribute('visibility', 'hidden');
            return;
        }

        const width = parseFloat(cell.getAttribute('width'))
        const height = parseFloat(cell.getAttribute('height'))
        const level = parseInt(cell.getAttribute('${FG_LEVEL_ATTR}'));

        _store_pos(cell);
        _store_pos(text);

        let relative_level = level - min_level;
        let new_x = (x - min_x) * scale;
        let new_y = graph_height - ((relative_level + 1) * cell_height)
        let new_width = width * scale;
        cell.setAttribute('x', new_x)
        cell.setAttribute('y', new_y)
        text.setAttribute('x', new_x)
        text.setAttribute('y', new_y)
        cell.setAttribute('width', new_width)
        cell.setAttribute('height', cell_height)
    });
    fg_trim_text(graph);
    fg_zoomed[graph] = true;
}

function fg_unzoom(graph, trim_text=true) {
    graph.querySelectorAll("g").forEach((group) => {
        const cell = group.querySelector('rect');
        const text = group.querySelector('text');

        if (group.hasAttribute("fg-hidden")) {
            group.removeAttribute('fg-hidden');
            cell.removeAttribute('visibility');
            text.removeAttribute('visibility');
        }
        else {
            _restore_pos(cell);
            _restore_pos(text);
        }
    });
    if (trim_text)
        fg_trim_text(graph);
    fg_zoomed[graph] = false;
}

function fg_init(fg_id) {
    fg_ids.push(fg_id);
    const graph = document.querySelector('#' + fg_id + '-graph');
    fg_viewBoxes[fg_id] = graph.getAttribute('viewBox');
    const output = document.querySelector('#' + fg_id + '-output')
    const detail = document.querySelector('#' + fg_id + '-detail')
    const unzoom = document.querySelector('#' + fg_id + '-unzoom')
    graph.querySelectorAll("g").forEach((group) => {
        const cell = group.querySelector('rect');
        cell.onmouseover = function () { fg_cell_mouseover(group, output, detail); };
        cell.onmouseout = function () { fg_cell_mouseout(group); };
        cell.onclick = function() { fg_zoom(graph, group); };
        unzoom.onclick = function () { fg_unzoom(graph); };

        // add text box
        let text = document.createElementNS("http://www.w3.org/2000/svg", 'text');
        text.setAttribute('x', parseFloat(cell.getAttribute('x')) + 5);
        text.setAttribute('y', cell.getAttribute('y'));
        text.setAttribute('dy', (parseFloat(cell.getAttribute('height')) / 2) + 2);
        group.append(text);
    });
    fg_trim_text(graph);
    fg_zoomed[graph] = false;
}
