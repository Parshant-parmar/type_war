const crypto = require("crypto");
const fs = require("fs");
const http = require("http");
const path = require("path");

const PORT = Number(process.env.PORT || 8080);
const ROOT = __dirname;
const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_MESSAGE_BYTES = 64 * 1024;
const rooms = new Map();

const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon"
};

const server = http.createServer((req, res) => {
  let pathname = "/";
  try {
    pathname = new URL(req.url, `http://${req.headers.host || "localhost"}`).pathname;
  } catch (error) {
    res.writeHead(400);
    res.end("Bad request");
    return;
  }

  if(pathname === "/") pathname = "/index.html";

  const filePath = path.normalize(path.join(ROOT, decodeURIComponent(pathname)));
  if(filePath !== ROOT && !filePath.startsWith(ROOT + path.sep)){
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }

  fs.readFile(filePath, (error, data) => {
    if(error){
      res.writeHead(404);
      res.end("Not found");
      return;
    }

    const type = mimeTypes[path.extname(filePath).toLowerCase()] || "application/octet-stream";
    res.writeHead(200, { "Content-Type": type });
    res.end(data);
  });
});

server.on("upgrade", (req, socket) => {
  const pathname = (req.url || "").split("?")[0];
  if(pathname !== "/ws"){
    socket.destroy();
    return;
  }

  const key = req.headers["sec-websocket-key"];
  if(!key){
    socket.destroy();
    return;
  }

  const accept = crypto.createHash("sha1").update(key + GUID).digest("base64");
  socket.write([
    "HTTP/1.1 101 Switching Protocols",
    "Upgrade: websocket",
    "Connection: Upgrade",
    `Sec-WebSocket-Accept: ${accept}`,
    "",
    ""
  ].join("\r\n"));

  const client = {
    id: crypto.randomBytes(4).toString("hex"),
    name: "Player",
    room: null,
    buffer: Buffer.alloc(0),
    socket
  };

  socket.on("data", chunk => readFrames(client, chunk));
  socket.on("close", () => leaveRoom(client));
  socket.on("error", () => leaveRoom(client));
});

function readFrames(client, chunk){
  client.buffer = Buffer.concat([client.buffer, chunk]);

  while(client.buffer.length >= 2){
    const frame = parseFrame(client.buffer);
    if(!frame) return;

    client.buffer = frame.remaining;

    if(frame.opcode === 0x8){
      leaveRoom(client);
      try { client.socket.end(); } catch (error) {}
      return;
    }

    if(frame.opcode === 0x9){
      writeFrame(client.socket, frame.payload, 0xA);
      continue;
    }

    if(frame.opcode !== 0x1) continue;
    handleText(client, frame.payload.toString("utf8"));
  }
}

function parseFrame(buffer){
  const first = buffer[0];
  const second = buffer[1];
  const opcode = first & 0x0f;
  const masked = (second & 0x80) === 0x80;
  let length = second & 0x7f;
  let offset = 2;

  if(length === 126){
    if(buffer.length < offset + 2) return null;
    length = buffer.readUInt16BE(offset);
    offset += 2;
  } else if(length === 127){
    if(buffer.length < offset + 8) return null;
    length = Number(buffer.readBigUInt64BE(offset));
    offset += 8;
  }

  if(length > MAX_MESSAGE_BYTES) return null;

  const maskOffset = offset;
  if(masked) offset += 4;
  if(buffer.length < offset + length) return null;

  let payload = buffer.subarray(offset, offset + length);
  if(masked){
    const mask = buffer.subarray(maskOffset, maskOffset + 4);
    payload = Buffer.from(payload.map((byte, index) => byte ^ mask[index % 4]));
  }

  return {
    opcode,
    payload,
    remaining: buffer.subarray(offset + length)
  };
}

function handleText(client, text){
  let msg = null;
  try { msg = JSON.parse(text); } catch (error) { return; }

  if(msg.type === "join"){
    joinRoom(client, msg.room, msg.name);
    return;
  }

  if(!client.room){
    send(client, { type: "error", message: "Join a room first." });
    return;
  }

  if(msg.type === "race" && typeof msg.text === "string"){
    broadcast(client.room, {
      type: "race",
      from: client.id,
      name: client.name,
      text: msg.text.slice(0, 20000),
      settings: normalizeSettings(msg.settings)
    }, client);
  } else if(msg.type === "state"){
    broadcast(client.room, {
      type: "state",
      from: client.id,
      name: client.name,
      state: normalizeState(msg.state)
    }, client);
  }
}

function joinRoom(client, value, name){
  const code = cleanRoom(value);
  const label = cleanName(name);

  if(client.room) leaveRoom(client);

  let members = rooms.get(code);
  if(!members){
    members = new Set();
    rooms.set(code, members);
  }

  if(members.size >= 2){
    send(client, { type: "error", message: `Room ${code} is full.` });
    return;
  }

  client.room = code;
  client.name = label;
  members.add(client);

  send(client, {
    type: "joined",
    playerId: client.id,
    room: code,
    players: playersFor(code)
  });

  broadcast(code, {
    type: "room",
    room: code,
    players: playersFor(code)
  });
}

function leaveRoom(client){
  if(!client.room) return;

  const code = client.room;
  const members = rooms.get(code);
  client.room = null;

  if(!members) return;
  members.delete(client);

  if(members.size === 0){
    rooms.delete(code);
    return;
  }

  broadcast(code, { type: "peer-left", playerId: client.id });
  broadcast(code, {
    type: "room",
    room: code,
    players: playersFor(code)
  });
}

function broadcast(room, payload, except){
  const members = rooms.get(room);
  if(!members) return;

  for(const client of members){
    if(client === except) continue;
    send(client, payload);
  }
}

function send(client, payload){
  if(!client.socket || client.socket.destroyed) return;
  writeFrame(client.socket, Buffer.from(JSON.stringify(payload)), 0x1);
}

function writeFrame(socket, payload, opcode){
  const length = payload.length;
  let header = null;

  if(length < 126){
    header = Buffer.from([0x80 | opcode, length]);
  } else if(length < 65536){
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(length, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(length), 2);
  }

  socket.write(Buffer.concat([header, payload]));
}

function playersFor(room){
  const members = rooms.get(room);
  if(!members) return [];
  return Array.from(members, client => ({ id: client.id, name: client.name }));
}

function cleanRoom(value){
  return String(value || "TYPEWAR").toUpperCase().replace(/[^A-Z0-9_-]/g, "").slice(0, 16) || "TYPEWAR";
}

function cleanName(value){
  return String(value || "Player").replace(/[^\w -]/g, "").trim().slice(0, 20) || "Player";
}

function normalizeSettings(settings){
  const src = settings || {};
  const difficulty = ["easy", "medium", "code"].includes(src.difficulty) ? src.difficulty : "easy";
  const timeMode = [0, 15, 30, 60, 120].includes(Number(src.timeMode)) ? Number(src.timeMode) : 0;
  const wordCount = [10, 25, 50, 75, 100].includes(Number(src.wordCount)) ? Number(src.wordCount) : 25;
  return { difficulty, timeMode, wordCount };
}

function normalizeState(state){
  const src = state || {};
  return {
    progress: clamp(Number(src.progress) || 0, 0, 1),
    wpm: clamp(Number(src.wpm) || 0, 0, 999),
    accuracy: clamp(Number(src.accuracy) || 100, 0, 100),
    errors: clamp(Number(src.errors) || 0, 0, 9999),
    started: !!src.started,
    finished: !!src.finished,
    elapsed: clamp(Number(src.elapsed) || 0, 0, 9999)
  };
}

function clamp(value, min, max){
  return Math.max(min, Math.min(max, value));
}

server.listen(PORT, () => {
  console.log(`Type War server running at http://localhost:${PORT}`);
});
