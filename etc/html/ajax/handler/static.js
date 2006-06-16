addEvent(window, "load", sortablesInit);

var debug_div;
var SORT_ASC = true;

function debug(msg)
{
	if( !debug_div )
	{
		debug_div = document.createElement( 'div' );
		var bodies = document.getElementsByTagName( 'BODY' );
		bodies[0].insertBefore( debug_div, bodies[0].firstChild );
	}
	debug_div.innerHTML += msg + "<br/>";
}

function sortablesInit()
{
	if( !document.getElementsByTagName ) return;
	var tbls = document.getElementsByTagName( 'table' );
	for(var i = 0; i < tbls.length; i++ ) {
		var tbl = tbls[i];
		if( ((' '+tbl.className+' ').indexOf( 'sortable' ) != -1) &&
			tbl.id )
			makeSortable(tbl);
	}
}

function makeSortable(table)
{
	if( !table.rows || table.rows.length == 0 )
		return;
		
	var row = table.rows[0];
	for(var i = 0; i < row.cells.length; i++)
	{
		var td = row.cells[i];
		var txt = innerText(td);
		td.innerHTML = '<a href="#" class="sortheader" ' +
			'onclick="sortTable(this,'+i+'); return false;">' +
			txt + '<span class="sortarrow">&nbsp;&nbsp;&nbsp;</span></a>';
	}
}

function sortTable(lnk, col)
{
	var th = lnk.parentNode;
	var tr = th.parentNode;
	var table = tr.parentNode;

	var body = table.parentNode;
	var table_after = table.afterSibling;
	body.removeChild( table );

	var rows = new Array();
	for(var i = 1; i < table.rows.length;)
	{
		var row = new Array();
		var s = getRowSpan(table.rows[i]);
		while(s--)
			row.push( table.rows[i++] );
		row.key = getRowValue(row[0],col);
		rows.push( row );
	}
	rows.sort(cmpRows);
	for(var i = 0; i < rows.length; i++)
		for(var j = 0; j < rows[i].length; j++)
			table.appendChild( rows[i][j] );

	body.insertBefore( table, table_after );
}

function cmpRows(i,j)
{
	return i.key > j.key ? 1 : i.key < j.key ? -1 : 0;
}

function getRowSpan(row)
{
	var max = 1;
	for(var i = 0; i < row.cells.length; i++)
	{
		var s = row.cells[i].getAttribute( 'rowspan' );
		if( s && s > max )
			max = s;
	}
	return max;
}

function getRowCol(row,col)
{
	var i = 0;
	while(col > 0)
	{
		var s = row.cells[i++].getAttribute( 'colspan' );
		if( s && s > 1 )
			col -= s
		else
			col--;
	}
	return row.cells[i];
}

var nonNumeric = /[^0-9]/;

function getRowValue(row,col)
{
	var value;
	var td = getRowCol(row,col);
	// Cache and return the value
	if( td )
	{
		td = innerText(td);
		return td.match(nonNumeric) ? td : Number(td);
	}
	else
		return null;
}

function innerText(ele)
{
	var str = ele.innerHTML;
	return str.replace(/<\/?[^>]+>/gi, '');
}

function addEvent(elm, evType, fn, useCapture)
// addEvent and removeEvent
// cross-browser event handling for IE5+,  NS6 and Mozilla
// By Scott Andrew
{
  if (elm.addEventListener){
    elm.addEventListener(evType, fn, useCapture);
    return true;
  } else if (elm.attachEvent){
    var r = elm.attachEvent("on"+evType, fn);
    return r;
  } else {
    alert("Handler could not be removed");
  }
}
