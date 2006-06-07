function debug(msg)
{
	var e = document.getElementById( "debug" );
	if( !e ) return;
	e.innerHTML = e.innerHTML + msg + "\n";
}

var xmlHttpPool = new Array(0);

function stateChange()
{
	// debug("onreadystatechange");
	for(i = 0; i < xmlHttpPool.length; i++) {
		if( xmlHttpPool[i].readyState == 4 )
		{
			updatePage( xmlHttpPool[i].responseText );
			xmlHttpPool.splice(i,1);
			i--;
		}
	}
	if( xmlHttpPool.length == 0 )
	{
		var e = document.getElementById( "done_message" );
		if( e )
			e.style.display = 'block';
	}
	// debug(xmlHttp + ".onreadystatechange: " + xmlHttp.readyState + ": " + xmlHttp.responseText);
}

function updatePage( response )
{
	var vals = response.split(" ");
	var url = vals[0];
	var success = vals[1];
	
	var td = document.getElementById( url );
	if( !td ) {
		alert('Unable to locate entry: ' + url);
		return;
	}
	
	if( success == 1 )
	{
		td.className = 'state passed';
		td.innerHTML = 'Passed';
	}
	else
	{
		td.className = 'state failed';
		td.innerHTML = 'Failed';
	}
}

function processUrls()
{
	var form = document.getElementById( "base_url_form" );
	if( !form ) {
		alert("Unable to locate base_url_form element");
		return;
	}
	var script = form.action + "?ajax=1";
	var base_urls = document.getElementsByName( "base_url" );
	
	// var val = document.getElementById( "id" ).value;
	for(var i = 0; i < base_urls.length; i++)
	{
		var url = script + "&base_url=" + escape(base_urls[i].value);
		// debug(url);

		var xmlHttp = ajaxOpen();
		if( !xmlHttp ) {
			alert("Failed to create HTTP request object");
			return;
		}
		xmlHttpPool.push(xmlHttp);

		xmlHttp.open("GET", url, true);

		xmlHttp.onreadystatechange = stateChange;
		// xmlHttp.setRequestHeader( 'key', 'value' );

		xmlHttp.send(null);
	}
}
