# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from wsgiref.util import FileWrapper
from django.shortcuts import render
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from .forms import IndexForm

import json
from collections import OrderedDict

from IndexSearch import Search
from IndexSearch import Index

def home(request):
	message = 'Welcome to fantasticsearch HomePage'
	return render(request, 'search.html', {'message': message})


@csrf_exempt
def search(request):
	message = ''
	if request.method == 'GET':
		query_term = request.GET.get('q',None)
    		query_type = request.GET.get('t','term')
    		query_field = request.GET.get('f','data')
    		if query_term == None or query_type not in ['phrase','term'] or query_field not in ['data','title']:
    			message = 'Invalid parameters passed (q='+query_term+', t='+query_type+', f='+query_field+')'
			return HttpResponse(content=message, content_type='text/plain', status=400)
	else:
		message = 'Invalid request method '+request.method
		return HttpResponse(content=message, content_type='text/plain', status=405)
	
	search = Search()
        message,code = search.searchJSON(query_term, query_type, query_field)
	if message == None:
		message = {"msg":"No documents match your search criteria"}
	return HttpResponse(content=json.dumps(message, indent=4), content_type='application/json', status=code)


@csrf_exempt
def index(request):
	if request.method == 'POST':
		try:
			doc = json.loads(request.body.decode("utf-8"), object_pairs_hook=OrderedDict)
                except:
			message = 'Invalid document provided'
                        return HttpResponse(content=message, content_type='text/plain', status=400)
		heads = ['id', 'title', 'data']
		if set(heads) > set(doc.keys()) or doc['id'] == None or doc['title'] == None or doc['data'] == None:
			message = 'Invalid document provided'
			return HttpResponse(content=message, content_type='text/plain', status=400)
	else:
                message = 'Invalid request method '+request.method
		return HttpResponse(content=message, content_type='text/plain', status=405)
	
	index = Index()
	message,code = index.indexJSON(doc)
	return HttpResponse(content=message, content_type='text/plain', status=code)
