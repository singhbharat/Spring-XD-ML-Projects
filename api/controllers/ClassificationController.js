var sformat=require('stringformat');
var propParser = require('properties-parser');
var config = propParser.read("./config/config.properties");
var restify = require('restify-clients');
var stringify=require('json-stringify');


//global variable
//var springxd_host =  config.springxd_host;
//var springxd_port =  config.springxd_port;
var mqtt_host = config.mqtt_host;
var mqtt_port = config.mqtt_port;


//SPRINGXD
//var springxd_url = "http://"+springxd_host+":"+springxd_port;
var rclient = restify.createStringClient({
	url: 'http://localhost:9393',
	//url: springxd_url,
  	accept : 'application/json',
  	headers : { 
  		"Access-Control-Allow-Origin" : '*', 
  		"Content-Type" : "application/x-www-form-urlencoded"
  	}
});



//model name
var model='iris-flower-classifier-1';

//source definition
var mqtt_source_definition="mqtt --url={0}:{1} --topics={2} --clientId={3}";

//machine learning model
var model_definition="analytic-pmml --modelname={0}";

//sink definition
var sink_definition="file"


//pipe operator
var module_pipe_separator = " | ";
exports.module_pipe_separator = module_pipe_separator;

//mqtt source definition
function get_mqtt_source_definition(req,callback1) {
	var sails = req._sails;
	var params=req.params.all();
	var topic_name=params.topic_name;
	var clientId = getClientId(req);

	var source_definition  = sformat(mqtt_source_definition,mqtt_host,mqtt_port,topic_name,clientId);
	return callback1(null, source_definition);
}
exports.get_mqtt_source_definition = get_mqtt_source_definition;

//function model_name
function get_model_name(req,callback2){
	var sails=req._sails;
	var params=req.params.all();
	var model_name=model; //right now defined as a global variable, later we can provide as a provision to user to choose this model
	var ml_model_definition=sformat(model_definition,model_name);
	console.log("ml_model_definition-->"+ml_model_definition);
	return callback2(null,ml_model_definition);
}
exports.get_model_name=get_model_name;


//return header value
 function getClientId(req) {
	var clientId = req.header("clientId");
	var sails = req._sails;
	return clientId;
}
exports.getClientId = getClientId

//stream life cycle
//1. create streams
function createStreams(req, res, definition, callback3) {
    var sails = req._sails;
	var params = req.params.all();
	var stream_name = params.stream_name;
	var deploy = false;
	var stream = {};
    stream.name = stream_name;
    console.log("stream.name="+stream.name);
    stream.definition = definition;
    console.log("stream.definition="+stream.definition);
    stream.deploy = deploy;
   // console.log("stream.deploy="+stream.deploy);
    var final_stream=JSON.stringify(stream);
    console.log("stream-->"+final_stream);
    //console.log("rclient-->"+JSON.stringify(rclient));
    rclient.post('/streams/definitions/'+stream , function (err, rq, rs, success) {
    	if (err) {return res.json("error during stream creation");}	
    	console.log("check2");
    	var error = null;
    	var result = "";
    	return callback3(error, result);
    });
}
exports.createStreams = createStreams;

//2.deploy streams
function deployStreams(req, res, stream_name, callback) {
	    var sails = req._sails;
		rclient.post('/streams/deployments/'+stream_name , function (err, rq, rs, success) {
			if (err) {return utils.errorHandler(err, req, res);};	
			var error = null;
	    	var result = "";
	    	return callback(error, result);
        });
}
exports.deployStreams = deployStreams;

//3.undeploy streams
function undeployStreams (req, res, stream_name, callback) {
	    var sails = req._sails;
	 	rclient.del('/streams/deployments/'+ stream_name , function (err, rq, rs, success) {
			if (err) {return utils.errorHandler(err, req, res);};	
			var error = null;
	    	var result = "";
	    	return callback(error, result);
		});
}
exports.undeployStreams = undeployStreams;

//4.destroy streams
function destroyStreams (req, res, stream_name, callback) {
		var sails = req._sails;
		rclient.del('/streams/definitions/'+ stream_name , function (err, rq, rs, success) {
			if (err) {return utils.errorHandler(err, req, res);};	
			var error = null;
	    	var result = "";
	    	return callback(error, result);
		});
}
exports.destroyStreams = destroyStreams;
/******************************************************************************************************************/

//NifiController
module.exports={
	create:function(req,res){
		var params=req.params.all();
		var stream_name=params.stream_name;
		get_mqtt_source_definition(req,function callback1(err,result){
			if(err){return res.json("error");}
			else if(result){
				var mqtt_source_definition=result;
				console.log("mqtt-->"+mqtt_source_definition);
				get_model_name(req,function callback2(err1,result1){
					if(err){return res.json("error1");}
					else if(result1){
						var ml_model_definition=result1;
						console.log("model-->"+ml_model_definition);
						var definition=mqtt_source_definition+" --outputType=application/x-xd-tuple"+module_pipe_separator+ml_model_definition+" --location=/home/hadoop/Downloads/iris-flower-classification-naive-bayes-1.pmml.xml --inputFieldMapping='sepalLength:Sepal.Length,sepalWidth:Sepal.Width,petalLength:Petal.Length,petalWidth:Petal.Width' --outputFieldMapping='Predicted_Species:predictedSpecies'"+module_pipe_separator+sink_definition;
						console.log("definition-->"+definition);
						createStreams(req,res, definition,function callback3(err2, result2) {
							if (err2) {return res.json("error2");}
							else if(result2){
								console.log("check1");
								var msg = {
									message : 'Analytic PMML stream has created',
					  	        	stream_name: stream_name	
					  	        };
					  	        return res.json(msg,200);
					  	    }else{return res.json("error2");}
					  	});
					}else{return res.json("error1");}
				});
			}else{return res.json("error");}
		});
	}
};