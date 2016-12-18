/**
 * Created by Alex on 18-12-2016.
 */
showNoNodeSelected();
var jsonData;
$.getJSON("data/details.json", function(json) {
    jsonData = json;

    // Load analysis details immediately
    if (jsonData) {
        console.log(jsonData);
        document.getElementById("analysesDetailsStartTime").innerHTML = jsonData.time_start;
        document.getElementById("analysesDetailsEndTime").innerHTML = jsonData.time_end;
        document.getElementById("analysesDetailsDuration").innerHTML = jsonData.total_duration;
        document.getElementById("analysesDetailsTotalLOC").innerHTML = jsonData.loc_total;
        document.getElementById("analysesDetailsDuplicatedLOC").innerHTML = jsonData.loc_duplicate;
        document.getElementById("analysesDetailsTotalClones").innerHTML = jsonData.total_clones;
    }
});

loadCloneDetails = function (path) {
    if (path.endsWith("java")) {
        var cloneDetails = getDetailsForPath(path);
        console.log(cloneDetails);
        if (cloneDetails) {
            showNodeDetails(path, cloneDetails);
            console.log("showing node details");
        } else {
            showNoNodeSelected();
            console.log("showing no node 1");
        }
    } else {
        showNoNodeSelected();
        console.log("showing no node 2");
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
    $("#selectedCloneDetails").hide();
}

function showNodeDetails(path, cloneDetails) {
    document.getElementById("selectedCloneText").innerHTML = path;
    document.getElementById("selectedCloneClonesFound").innerHTML = cloneDetails.clones.length;
    document.getElementById("selectedCloneDuplicatedLOC").innerHTML = cloneDetails.loc_duplicate;
    $("#selectedCloneDetails").show();
}