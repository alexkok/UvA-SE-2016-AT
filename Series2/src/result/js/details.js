/**
 * Created by Alex on 18-12-2016.
 */
showNoNodeSelected();
var jsonData;
$.getJSON("data/details.json", function(json) {
    jsonData = json;
    console.log(jsonData);
    // Load analysis details immediately
    if (jsonData) {
        document.getElementById("analysesDetailsStartTime").innerHTML = jsonData.time_start;
        document.getElementById("analysesDetailsEndTime").innerHTML = jsonData.time_end;
        document.getElementById("analysesDetailsDuration").innerHTML = jsonData.total_duration;
        document.getElementById("analysesDetailsDuplicatedLOC").innerHTML = jsonData.loc_duplicate;
        document.getElementById("analysesDetailsTotalClones").innerHTML = jsonData.total_clones;
    }
});

loadCloneDetails = function (path) {
    if (path.endsWith("java")) {
        var cloneDetails = getDetailsForPath(path);
        if (cloneDetails) {
            showNodeDetails(path, cloneDetails);
        } else {
            showNoNodeSelected();
        }
    } else {
        showNoNodeSelected();
    }
};

function getDetailsForPath(path) {
    for (var i = 0; i < jsonData.clones.length; i++) {
        if (jsonData.clones[i].path === path) {
            return jsonData.clones[i];
        }
    }
    return undefined;
}

function showNoNodeSelected() {
    document.getElementById("selectedCloneText").innerHTML = "No node selected";
    $(".selectedCloneDetails").hide();
}

function showNodeDetails(path, cloneDetails) {
    document.getElementById("selectedCloneText").innerHTML = path;
    document.getElementById("selectedCloneClonesFound").innerHTML = cloneDetails.clones.length;
    document.getElementById("selectedCloneDuplicatedLOC").innerHTML = cloneDetails.loc_duplicate;
    $(".selectedCloneDetails").show();

    var cloneDetailPanel = document.getElementById("selectedCloneDetailsPanel");
    // Clear old childs
    while (cloneDetailPanel.firstChild) {
        cloneDetailPanel.removeChild(cloneDetailPanel.firstChild);
    }
    // For each clone: create html object and add it to the body
    for (var i = 0; i < cloneDetails.clones.length; i++) {
        var clonePanel = createHTMLObjectForClone(i, cloneDetails.clones[i]);
        cloneDetailPanel.appendChild(clonePanel);
    }

}
function createHTMLObjectForClone(id, clone) {
    var panel = document.createElement("div");
    panel.className = "panel panel-default";
    // The heading
    var panelHeading = document.createElement("div");
    panelHeading.className = "panel-heading";
    var panelHeadingCollapse = document.createElement("a");
    panelHeadingCollapse.setAttribute("data-toggle", "collapse");
    panelHeadingCollapse.setAttribute("href", "#collapse" + id);
    var panelHeadingTitle = document.createElement("h4");
    panelHeadingTitle.className = "panel-title";
    // panelHeadingTitle.innerHTML = "xx lines (xx - xx)";
    panelHeadingTitle.innerHTML = clone.total_lines + " lines (" + clone.line_start + " - " + clone.line_end + ")";
    panelHeadingCollapse.appendChild(panelHeadingTitle);
    panelHeading.appendChild(panelHeadingCollapse);
    // The body
    var collapsingBody = document.createElement("div");
    collapsingBody.id = "collapse" + id;
    collapsingBody.className = "panel-collapse collapse";
    var panelBody= document.createElement("div");
    panelBody.className = "panel-body";
    panelBody.innerHTML = clone.content;
    collapsingBody.appendChild(panelBody);

    // Adding heading and body to the panel
    panel.appendChild(panelHeading);
    panel.appendChild(collapsingBody);
    return panel;
}