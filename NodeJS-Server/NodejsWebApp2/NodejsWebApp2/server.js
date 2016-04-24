// var http = require("http");
// var url = require('url');
// var fs = require('fs');
// var io = require('socket.io');
// var mongoose = require('mongoose');
// var db = require('./config/db');

// console.log("Starting");

// var server = http.createServer(function(request, response){
//     var path = url.parse(request.url).pathname;

//     switch(path){
//         case '/':
//             response.writeHead(200, {'Content-Type': 'text/html'});
//             response.write('hello world');
//             response.end();
//             break;
//         case '/socket.html':
//             fs.readFile(__dirname + path, function(error, data){
//                 if (error){
//                     response.writeHead(404);
//                     response.write("opps this doesn't exist - 404");
//                     response.end();
//                 }
//                 else{
//                     response.writeHead(200, {"Content-Type": "text/html"});
//                     response.write(data, "utf8");
//                     response.end();
//                 }
//             });
//             break;
//         default:
//             response.writeHead(404);
//             response.write("opps this doesn't exist - 404");
//             response.end();
//             break;
//     }
// });

// server.listen(8001);

// var listener = io.listen(server);

// listener.sockets.on('connection', function(socket){
// 	console.log("New connection");
// 	connectionsubject = mongoose.createConnection(db.urlSubjectViews);

//   //send data to client
// //   setInterval(function(){
// //     socket.emit('date', {'date': connectionsubject
// //     	// new Date()
// // });
// //   }, 1000);

//     socket.emit('date', {'date': new Date()});
//     	// new Date()



//   //recieve client data
//   socket.on('client_data', function(data){
//     process.stdout.write(data.letter);
//   });
// });





// var express = require('express');
// var http = require('http');
// var io = require('socket.io');
 
 
// var app = express();
// app.use(express.static('./public'));
// //Specifying the public folder of the server to make the html accesible using the static middleware
 
// var server =http.createServer(app).listen(8124);
// //Server listens on the port 8124
// io = io.listen(server); 
// /*initializing the websockets communication , server instance has to be sent as the argument */
 
// io.sockets.on("connection",function(socket){
//     /*Associating the callback function to be executed when client visits the page and 
//       websocket connection is made */
      
//       var message_to_client = {
//         data:"Connection with the server established"
//       }
//       socket.send(JSON.stringify(message_to_client)); 
//       /*sending data to the client , this triggers a message event at the client side */
//     console.log('Socket.io Connection with the client established');
//     socket.on("message",function(data){
//         /*This event is triggered at the server side when client sends the data using socket.send() method */
//         data = JSON.parse(data);
 
//         console.log(data);
//         /*Printing the data */
//         var ack_to_client = {
//         data:"Server Received the message"
//       }
//       socket.send(JSON.stringify(ack_to_client));
//         /*Sending the Acknowledgement back to the client , this will trigger "message" event on the clients side*/
//     });
 
// });

// //var http = require('http');
// //var port = process.env.port || 1337;
// //http.createServer(function (req, res) {
// //    res.writeHead(200, { 'Content-Type': 'text/plain' });
// //    res.end('Hello World\n');
// //}).listen(port);

// var express = require('express');
// var app = express.createServer();

// app.use(express.bodyParser());

// /*app.get('/endpoint', function(req, res){
// 	var obj = {};
// 	obj.title = 'title';
// 	obj.data = 'data';
	
// 	console.log('params: ' + JSON.stringify(req.params));
// 	console.log('body: ' + JSON.stringify(req.body));
// 	console.log('query: ' + JSON.stringify(req.query));
	
// 	res.header('Content-type','application/json');
// 	res.header('Charset','utf8');
// 	res.send(req.query.callback + '('+ JSON.stringify(obj) + ');');
// });*/

// app.get('/endpoint', function(req, res){
// 	var obj = {};
// 	console.log('body: ' + JSON.stringify(req.body));
// 	res.send(req.body);
// });


// app.listen(8080);


// modules =================================================
var express = require('express');
var app = express();
var mongoose = require('mongoose');
var bodyParser = require('body-parser');
var methodOverride = require('method-override');

// configuration ===========================================

app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});



// config files
var port = process.env.PORT || 8080; // set our port
var db = require('./config/db');

// connect to our mongoDB database (commented out after you enter in your own credentials)
connectionsubject = mongoose.createConnection(db.urlSubjectViews);


// get all data/stuff of the body (POST) parameters
app.use(bodyParser.json()); // parse application/json 
app.use(bodyParser.json({ type: 'application/vnd.api+json' })); // parse application/vnd.api+json as json
app.use(bodyParser.urlencoded({ extended: true })); // parse application/x-www-form-urlencoded

app.use(methodOverride('X-HTTP-Method-Override')); // override with the X-HTTP-Method-Override header in the request. simulate DELETE/PUT
app.use(express.static(__dirname + '/public')); // set the static files location /public/img will be /img for users

// routes ==================================================
require('./app/route')(app); // pass our application into our routes

 

// start app ===============================================
app.listen(port);
console.log('Magic happens on port ' + port); 			// shoutout to the user
exports = module.exports = app; 				    		// expose app


