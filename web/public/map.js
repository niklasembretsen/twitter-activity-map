document.addEventListener('DOMContentLoaded', function() {
	fetch('/data').then(function(response) {
		return response.json();
	}).then(function(json) {
		console.log(json);
	});
});