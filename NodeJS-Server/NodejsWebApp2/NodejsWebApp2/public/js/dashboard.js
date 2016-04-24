queue()
    .defer(d3.json, "/api/data")
    .await(makeGraphs);

var diameter = 500, //max size of the bubbles
    color = d3.scale.category20b(); //color category

var bubble = d3.layout.pack()
    .sort(null)
    .size([diameter, diameter])
    .padding(1.5);

var svg = d3.select("#chart")
    .append("svg")
    .attr("width", diameter)
    .attr("height", diameter)
    .attr("class", "bubble");



function makeGraphs(error, apiData) {
    
    'use strict';
    
    console.log(apiData);
    var dataset = apiData;
    //convert numerical values from strings to numbers
    var data = dataset.map(function (d) { d.value = +d["value"]; return d; });
    
    //bubbles needs very specific format, convert data to this.
    var nodes = bubble.nodes({ children: data }).filter(function (d) { return !d.children; });
    
    //setup the chart
    var bubbles = svg.append("g")
        .attr("transform", "translate(0,0)")
        .selectAll(".bubble")
        .data(nodes)
        .enter();
    
    //create the bubbles
    bubbles.append("circle")
        .attr("r", function (d) { return d.r; })
        .attr("cx", function (d) { return d.x; })
        .attr("cy", function (d) { return d.y; })
        .style("fill", function (d) { return color(d.value); });
    
    //format the text for each bubble
    bubbles.append("text")
        .attr("x", function (d) { return d.x; })
        .attr("y", function (d) { return d.y + 5; })
        .attr("text-anchor", "middle")
        .text(function (d) { return d["label"]; })
        .style({
        "fill": "white", 
        "font-family": "Helvetica Neue, Helvetica, Arial, san-serif",
        "font-size": "12px"
    });
}