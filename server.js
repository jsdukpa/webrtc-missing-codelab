require('dotenv').config();
const fs = require('fs');
const http = require('http');

const WebSocket = require('ws');
const uuid = require('uuid');

const port = 8080;

// generate twilio token
const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;
const twilio = require('twilio')(accountSid, authToken);


// We use a HTTP server for serving static pages. In the real world you'll
// want to separate the signaling server and how you serve the HTML/JS, the
// latter typically through a CDN.
const server = http.Server({})
    .listen(port);
server.on('listening', () => {
    console.log('Server listening on http://localhost:' + port);
});
server.on('request', (request, response) => {
    fs.readFile('static/index.html', (err, data) => {
        if (err) {
            console.log('could not read client file', err);
            response.writeHead(404);
            response.end();
            return;
        }
        response.writeHead(200, { 'Content-Type': 'text/html' });
        response.end(data);
    });
});

// A map of websocket connections.
const connections = new Map();
// WebSocket server, running alongside the http server.
const wss = new WebSocket.Server({ server });

// map of calls
const calls = new Map();

// Generate a (unique) client id.
// Exercise: extend this to generate a human-readable id.
function generateClientId() {
    // TODO: enforce uniqueness here instead of below.
    return uuid.v4();
}

wss.on('connection', (ws) => {
    // Assign an id to the client. The other alternative is to have the client
    // pick its id and tell us. But that needs handle duplicates. It is preferable
    // if you have ids from another source but requires some kind of authentication.
    const id = generateClientId();
    console.log(id, 'Received new connection');


    let callId = null; // assume we can only have one call at a time for now

    if (connections.has(id)) {
        console.log(id, 'Duplicate id detected, closing');
        ws.close();
        return;
    }
    // Store the connection in our map of connections.
    connections.set(id, ws);

    // Send a greeting to tell the client its id.
    ws.send(JSON.stringify({
        type: 'hello',
        id,
    }));

    // Send an ice server configuration to the client. For stun this is synchronous,
    // for TURN it might require getting credentials.
    const stunServer = { urls: 'stun:stun.l.google.com:19302' };
    twilio.tokens.create().then(token => {
        console.log('Twilio token:', token);
        ws.send(JSON.stringify({
            type: 'iceServers',
            iceServers: [stunServer, ...token.iceServers],
        }));
    });

    // Remove the connection. Note that this does not tell anyone you are currently in a call with
    // that this happened. This would require additional statekeeping that is not done here.
    ws.on('close', () => {
        console.log(id, 'Connection closed');
        connections.delete(id);
    });

    ws.on('message', (message) => {
        console.log(id, 'received');
        let data;
        // TODO: your protocol should send some kind of error back to the caller instead of
        // returning silently below.
        try {
            data = JSON.parse(message);
        } catch (err) {
            console.log(id, 'invalid json', err, message);
            return;
        }
        if (!data.id) {
            console.log(id, 'missing id', data);
            return;
        }

        console.log(id, 'from: ', data.id, 'type:', data.type);

        // The direct lookup of the other clients websocket is overly simplified.
        // In the real world you might be running in a cluster and would need to send
        // messages between different servers in the cluster to reach the other side.
        if (!connections.has(data.id)) {
            console.log(id, 'peer not found', data.id);
            // TODO: the protocol needs some error handling here. This can be as
            // simple as sending a 'bye' with an extra error element saying 'not-found'.
            return;
        }
        
        const peer = connections.get(data.id);
        switch (data.type) {
            case 'offer':
                callId = createCall(id, data.id);
                sendMessage(peer, Object.assign({}, data, {id, callId}));
                break;
            case 'answer':
                answerCall(data.callId);
                callId = data.callId;
                sendMessage(peer, Object.assign({}, data, {id}));
                break;
            case 'bye':
                hangup(callId);
                sendMessage(peer, Object.assign({}, data, {id}));
                break;
            default:
                sendMessage(peer, Object.assign({}, data, {id}));
        }
    });

    function createCall(fromId, toId) {
        const call = {
            id: generateClientId(),
            status: 'calling',
            fromId,
            toId,
            startTime: undefined,
            endTime: undefined,
        };
        calls.set(call.id, call);
        console.log('call created', call);
        return call.id;
    }

    function answerCall(callId) {
        console.log('begin answer', callId);
        if (!calls.has(callId)) return;

        const call = calls.get(callId);
        call.status = 'answered';
        call.startTime = Date.now();

        console.log('call answered', call);
    }

    function hangup(callId) {
        console.log('begin hangup', callId);
        if (!calls.has(callId)) return;

        const call = calls.get(callId);
        call.status = 'hungup';
        call.endTime = Date.now();

        console.log('call hungup', call);
    }

    function sendMessage(peer, data) {
        // Stamp messages with our id. In the client-to-server direction, 'id' is the
        // client that the message is sent to. In the server-to-client direction, it is
        // the client that the message originates from.
        peer.send(JSON.stringify(data), (err) => {
            if (err) {
                console.log(id, 'failed to send to peer', err);
            }
        });
    }
});
