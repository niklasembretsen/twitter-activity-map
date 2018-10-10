document.addEventListener('DOMContentLoaded', function() {
	fetch('/data').then(function(response) {
		return response.json();
	}).then(function(json) {
		console.log(json);
	});

	setUpMap();

	// setTimeout(function(){
	// 	var point1 = projection([5,5]);
	// 	heat.data([[point1[0],point1[1],10000],[100,100,5000]]);
	// 	heat.draw(0.05);
	// 	console.log('now')
	// }, 2000);
});

window.addEventListener('resize', function() {
	// Need to recalculate with and height etc
	setUpMap();
});

var setUpMap = function () {
	const width = window.innerWidth;
	const height = window.innerHeight;

	// Reference to the dom-element
	div = d3.select('#map-wrapper');

	// Create the map svg(background)
	mapLayer = div.append('svg').attr('id', 'map').attr('width', width).attr('height', height);
	// Create the canvas(heat map)
	canvasLayer = div.append('canvas').attr('id', 'heatmap').attr('width', width).attr('height', height);

	canvas = canvasLayer.node();
	context = canvas.getContext("2d");

	// Used to calculate long, lat to pixels
	projection = d3.geoMercator().translate([width/2, height/2]);
	path = d3.geoPath(projection);

	// Fetch the countries and then draw map
	d3.queue()
		.defer(d3.json, '/static/world-50m.json')
		.await(drawMap);
}

var drawMap =  function(error, world, dests) {
	// Get countries from topojson
	var countries = topojson.feature(world, world.objects.countries).features;

	// Draw map
	mapLayer
		.append('g')
		.classed('countries', true)
		.selectAll(".country")
		  .data(countries)
		.enter()
		  .append("path")
		  .attr("class", "country")
		  .attr("d", path);
	drawHeatMap();
}

var drawHeatMap = function() {
	// convert long = 0, lat = 0 to pixels
	var point1 = projection([0,0]);

	// Init the heatmap
	heat = simpleheat(canvas);

	// Add reference data to the heatmap
	heat.data([[point1[0],point1[1],10000],[100,100,10000],[50,50,10000]])

	// set point radius and blur radius (25 and 15 by default)
	heat.radius(10, 10);

	// optionally customize gradient colors, e.g. below
	heat.gradient({0: '#114B5F', 0.5: '#E4FDE1', 1: '#F45B69'});

	// set maximum for domain
	heat.max(10000);

	// draw into canvas, with minimum opacity threshold
	heat.draw(0.05);
}
