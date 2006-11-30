window.onload = on_load;

function on_load()
{
	var details = document.getElementsByClassName( 'detail_link' );
	for(var i = 0; i < details.length; i++)
	{
		var ele = details[i];
		ele.onclick = onClickDetail;
		Element.addClassName(ele, 'clickable');
		ele.innerHTML = 'Show Detail';
	}
	var showall = document.getElementById( 'show_all_details' );
	showall.onclick = onClickShowAllDetails;
	Element.addClassName(showall, 'clickable');
	showall.innerHTML = 'Show All Details';
}

function onClickShowAllDetails(e)
{
	var details = document.getElementsByClassName( 'detail_link' );
	for(var i = 0; i < details.length; i++)
	{
		showDetail(details[i]);
	}
}

function onClickDetail(e)
{
	var ele = Event.element(e);
	showDetail(ele);
}

function showDetail(ele)
{
	var id = ele.getAttribute('target');
	var tgt = document.getElementById(id);
	if( tgt.style.display == 'block' )
	{
		ele.innerHTML = 'Show Detail';
		tgt.style.display = 'none';
		return;
	}
	else
	{
		ele.innerHTML = 'Hide Detail';
		tgt.style.display = 'block';
	}
	
	// Already got the detail, so just show it
	if( tgt.childNodes.length > 0 )
		return;
		
	var url = 'status';
	var myAjax = new Ajax.Updater(
		{success: id},
		url,
		{
			method: 'get',
			parameters: 'ajax=1&repository=' + id,
			onFailure: reportError
		}
	);
}

function reportError(request)
{
	alert('Error making Ajax request.');
}
