/**
 * @NApiVersion 2.x
 * @NScriptType ScheduledScript
 * @NAmdConfig  ./oauthShim.json
 */

import {EntryPoints} from 'N/types';

import * as file from 'N/file';
import * as log from 'N/log';
import * as record from 'N/record';
import * as runtime from 'N/runtime';
import * as search from 'N/search';
import * as task from 'N/task';

import * as s3Push from './kotnHandleS3Push';

function makeTS(d){
	var d = d || new Date();
	return d.toISOString().replace(/\.\d+/, '').replace(/[-:]/g, '');
}

function escapeCSV(val){
	if (!val) return '';
	return !(/[",\s]/).test(val) ? val : ('"' + val.replace(/"/g, '""') + '"');
}

export function execute(ctx){
	var me = runtime.getCurrentScript();
	if(runtime.envType == runtime.EnvType.SANDBOX){
		var sbEnabled = me.getParameter({name:'custscript_kotn_s3_enable_in_sandbox'});
		if(!sbEnabled){
			log.audit({
				title:me.id,
				details:'skipping sandbox run'
			});
			return;
		}
	}
	var searchId = parseInt(me.getParameter({name:'custscript_kotn_s3_search'}));
	var includeTS = me.getParameter({name:'custscript_kotn_s3_use_ts'});
	var lineLimit = parseInt(me.getParameter({name:'custscript_kotn_s3_lines'}),10) || 0;
	var deferredDeployment = me.getParameter({name:'custscript_kotn_s3_defer_dep'});
	var deferredFolder = me.getParameter({name:'custscript_kotn_s3_defer_folder'});

	var srch :search.Search = search.load({id: ''+searchId});

	var title = srch.title;
	if(!title){
		var srchInfo = search.lookupFields({
			type:search.Type.SAVED_SEARCH,
			id:searchId,
			columns:[
				'title'
			]
		});

		log.debug({
			title:'looked up title for '+ searchId,
			details:JSON.stringify(srchInfo)
		});

		title = srchInfo.title;
	}
	var name = title.replace(/\W/g, ' ').replace(/ +/g, ' ').replace(/ /g, '_');

	if(includeTS){
		name += '_'+ makeTS(new Date());
	}

	name += '.csv';

	if(deferredDeployment){
		var pathParts = [];
		var folderPath = null;
		var filters = [];
		if(deferredFolder){
			if(deferredFolder.indexOf('/') != -1) folderPath = deferredFolder;
			else {
				if(''+parseInt(deferredFolder) == deferredFolder){
					filters = [
						['internalid', 'is', deferredFolder], 'AND',
						['isinactive', 'is', 'F']
					]
				}else{
					filters = [
						['name', 'is', deferredFolder], 'AND',
						['isinactive', 'is', 'F']
					];
				}

				var nextParentId = null;
				search.create({
					type:'folder',
					filters:filters,
					columns:[
						'name',
						'parent'
					]
				}).run().each(f=>{
					pathParts.unshift(f.getValue({name:'name'}));
					var nextParentId = f.getValue({name:'parent'});
					return false;
				});
				while(nextParentId){
					var parentFolder = record.load({
						type:'folder',
						id:nextParentId
					});
					pathParts.unshift(<string>parentFolder.getValue({fieldId:'name'}));
					nextParentId = parentFolder.getValue({fieldId:'parent'});
				}
			}
			folderPath = pathParts.join('/');
		}

		var filePath = folderPath+ (folderPath.length ? '/' : '') + name;
		var searchTask = task.create({
			taskType: task.TaskType.SEARCH,
			savedSearchId: searchId,
			filePath: filePath
		});

		var dependency = task.create({
			taskType:task.TaskType.SCHEDULED_SCRIPT,
			scriptId:'customscript_kotn_s3_defer_transfer',
			deploymentId:deferredDeployment,
			params:{
				custscript_kotn_deferred_s3_folder:  me.getParameter({name:'custscript_kotn_s3_folder'}),
				custscript_kotn_deferred_s3_file: filePath
			}
		});

		searchTask.addInboundDependency(dependency);

		var taskId = searchTask.submit();
		log.audit({
			title:'queued '+ name,
			details: taskId
		});

		return;
	}

	var S3Parts = new s3Push.S3PartsPush(name, {
		S3Region: me.getParameter({name:'custscript_kotn_s3_region'}),
		S3Key : me.getParameter({name:'custscript_kotn_s3_key'}),
		S3Secret : me.getParameter({name:'custscript_kotn_s3_secret'}),
		S3Bucket : me.getParameter({name:'custscript_kotn_s3_bucket'}),
		S3Folder : me.getParameter({name:'custscript_kotn_s3_folder'}) || ''
	});

	var accum = "";
	var sentHeader = false;
	const pagedResults = srch.runPaged({pageSize:1000}); //5 units

	var linesReported = 0;

	pagedResults.pageRanges.forEach(function(pageRange){
		log.debug({
			title:name,
			details:'collecting page '+ pageRange.index
		});
		if(lineLimit && linesReported >= lineLimit) return;
		var myPage = pagedResults.fetch({index: pageRange.index}); //5 Units
		myPage.data.forEach(function(result){
			if(lineLimit && linesReported >= lineLimit) return;
			var cols = result.columns;
			if(!sentHeader){
				// add header
				var headerLine = cols.map(c=>{
					return escapeCSV(c.label || c.name);
				}).join(',');
				sentHeader = true;
				accum += headerLine;
			}
			linesReported++;
			var lineData = (cols.map(c=>{
				return escapeCSV(result.getText(c) || result.getValue(c));
			}).join(','));

			accum += '\n'+ lineData;
			//TEST to push sample file page over 5MB
			// for(var i = 0;i< 200; i++){
			// 	accum += '\n'+ lineData;
			// }

		});
		if(accum.length > s3Push.MIN_S3_PART_SIZE){
			S3Parts.push(accum);
			accum = "";
		}
	});

	S3Parts.finish(accum);

	if(!linesReported){
		log.audit({
			title:'no contents for '+name,
			details:null
		});
		return;
	}

	log.audit({
		title:'sending '+name,
		details:linesReported +' lines'
	});

};
