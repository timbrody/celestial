/*
 *
 * Javascript support for the identifiers graph
 *
 */

var docHdl;

var dontTrigger = false;
var SIZE = 0;

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
	SIZE = plot.getAttribute( '_size' );
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
			rect.onmousemove = msMoveBar;
			rect.origX = 1 * rect.getAttribute('x');
			rect.origWidth = 1 * rect.getAttribute('width');
		}
	}
}

function msOverBar(evt)
{
/*	if( dontTrigger )
		return;
	dontTrigger = true;
	window.setTimeout("dontTrigger = false;", 300); */
	
	var rect = evt.target;
	var x = evt.clientX; // From page's origin
	var y = evt.clientY;

	rect.flow = 0;
	rect.mx = x;
	rect.my = y;

	focusBar(rect, 0);
}

function msOutBar(evt)
{
/*	if( dontTrigger )
		return; */
	
	var rect = evt.target;
	unfocusBar(rect);
}

function msMoveBar(evt)
{
	var rect = evt.target;
	var x = evt.clientX; // From page's origin
	var y = evt.clientY;

	// var d = x < rect.mx ? -1 : (x > rect.mx ? 1 : 0);
	var d = x - rect.mx;
	rect.flow += d;
	rect.mx = x;
	rect.my = y;

	if( d != 0 )
		focusBar(rect, d);
}

function focusBar(bar, dir)
{
	var a = bar.parentNode.previousSibling;

	var scale = 20; // Pixels

	// This is correct, but it is impractical?
	//var flow = bar.flow * ((bar.origWidth + scale) / bar.origWidth);
	var flow = bar.flow;

	bar.setAttribute('width', bar.origWidth + scale);
	// bar.setAttribute('x', bar.origX - scale / 2 - flow);
	bar.setAttribute('x', bar.origX - scale / 2);

	var steps = 8;
	var step = Math.PI/steps;
	//var dx = scale/2 + flow;
	var dx = scale/2;
	for(var i = steps/2; a != null; i--)
	{
		if( a.nodeName != 'a' )
			a = a.previousSibling;
		if( a == null )
			break;
		var rect = a.firstChild;
		while( rect.nodeName != 'rect' )
			rect = rect.nextSibling;
		var width = 0;
		if( i > 0 )
		{
			width = Math.sin(i * step) * scale;
			dx += width;
		}
		rect.setAttribute('width', rect.origWidth + width);
		rect.setAttribute('x', rect.origX - dx);
		a = a.previousSibling;
	}

	a = bar.parentNode.nextSibling;
	//dx = scale/2 - flow;
	dx = scale/2;
	for(var i = steps/2; a != null; i++)
	{
		if( a.nodeName != 'a' )
			a = a.nextSibling;
		if( a == null )
			break;
		var rect = a.firstChild;
		while( rect.nodeName != 'rect' )
			rect = rect.nextSibling;
		var width = 0;
		if( i < steps )
		{
			width = Math.sin(i * step) * scale;
			dx += width;
		}
		rect.setAttribute('width', rect.origWidth + width);
		rect.setAttribute('x', rect.origX - width + dx);
		a = a.nextSibling;
	}
}

function unfocusBar(bar)
{
	if( dontTrigger )
		return;
	var a = bar.parentNode;
	var plot = a.parentNode;
	a = plot.firstChild;
	while( a != null )
	{
		if( a.nodeName != 'a' )
			a = a.nextSibling;
		if( a == null )
			break;
		var rect = a.firstChild;
		while( rect.nodeName != 'rect' )
			rect = rect.nextSibling;
		rect.setAttribute('width', rect.origWidth);
		rect.setAttribute('x', rect.origX);
		a = a.nextSibling;
	}
}
