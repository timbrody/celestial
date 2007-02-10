/*
 *
 * Javascript support for the identifiers graph
 *
 */

var docHdl;

function plotInit(evt)
{
	if( docHdl == null )
		docHdl = window.svgDocument; // Adobe SVG
	if( docHdl == null )
		docHdl = evt.target.ownerDocument; // Generic?
	if( docHdl == null )
	{
		alert("Error getting handle to document");
		return;
	}
	var plot = docHdl.getElementById( 'plot' );
	for(var i = 0; i < plot.childNodes.length; i++)
	{
		var a = plot.childNodes[i];
		if( a.nodeName != 'a' )
			continue;
		for(var j = 0; j < a.childNodes.length; j++)
		{
			var rect = a.childNodes[j];
			if( rect.nodeName != 'rect' )
				continue;
			rect.onmouseover = msOverBar;
			rect.onmouseout = msOutBar;
			rect.origX = rect.getAttribute('x');
		}
	}
}

function msOverBar(evt)
{
	var rect = evt.target;
	focusBar(rect);
}

function msOutBar(evt)
{
	var rect = evt.target;
	unfocusBar(rect);
}

function focusBar(bar)
{
	var a = bar.parentNode.previousSibling;

	var scale = a.parentNode.childNodes.length/75;
	bar.setAttribute('width', 1 + scale);
	bar.setAttribute('x', bar.origX - scale/2);

	var steps = 20;
	var step = Math.PI/steps;
	var dx = scale/2;
	for(var i = 0; i < steps/2; i++)
	{
		if( a.nodeName != 'a' )
			a = a.previousSibling;
		if( a == null )
			break;
		var rect = a.firstChild;
		while( rect.nodeName != 'rect' )
			rect = rect.nextSibling;
		var width = Math.sin((steps / 2 - i) * step) * scale;
		rect.setAttribute('width', 1 + width);
		dx += width;
		rect.setAttribute('x', rect.origX - dx);
		a = a.previousSibling;
	}

	a = bar.parentNode.nextSibling;
	dx = scale/2;
	for(var i = steps/2; i < steps; i++)
	{
		if( a.nodeName != 'a' )
			a = a.nextSibling;
		if( a == null )
			break;
		var rect = a.firstChild;
		while( rect.nodeName != 'rect' )
			rect = rect.nextSibling;
		var width = Math.sin(i * step) * scale;
		rect.setAttribute('width', 1 + width);
		dx += width;
		rect.setAttribute('x', rect.origX - width + dx);
		a = a.nextSibling;
	}
}

function unfocusBar(bar)
{
	var a = bar.parentNode;
	var steps = 22; // Above does steps + 2
	for(var i = 0; i < steps/2; i++)
	{
		if( a.nodeName != 'a' )
			a = a.previousSibling;
		if( a == null )
			break;
		var rect = a.firstChild;
		while( rect.nodeName != 'rect' )
			rect = rect.nextSibling;
		rect.setAttribute('width', 1);
		rect.setAttribute('x', rect.origX);
		a = a.previousSibling;
	}
	a = bar.parentNode;
	for(var i = steps/2; i < steps; i++)
	{
		if( a.nodeName != 'a' )
			a = a.nextSibling;
		if( a == null )
			break;
		var rect = a.firstChild;
		while( rect.nodeName != 'rect' )
			rect = rect.nextSibling;
		rect.setAttribute('width', 1);
		rect.setAttribute('x', rect.origX);
		a = a.nextSibling;
	}
}
