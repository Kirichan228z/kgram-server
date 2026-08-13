const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const db = new sqlite3.Database('kgram.db');

db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    phone TEXT UNIQUE,
    username TEXT,
    bio TEXT,
    avatar TEXT,
    is_online INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
  
  db.run(`CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sender_id INTEGER,
    receiver_id INTEGER,
    message TEXT,
    type TEXT DEFAULT 'text',
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
  
  db.run(`CREATE TABLE IF NOT EXISTS chats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user1_id INTEGER,
    user2_id INTEGER,
    last_message TEXT,
    last_message_time DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
});

const clients = new Map();
const userSockets = new Map();

wss.on('connection', (ws) => {
  console.log('Новое подключение');
  
  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      
      switch(msg.type) {
        case 'login':
          if (msg.user_id) {
            db.get('SELECT * FROM users WHERE id = ?', [msg.user_id], (err, user) => {
              if (user) {
                userSockets.set(ws, user);
                clients.set(user.id, ws);
                db.run('UPDATE users SET is_online = 1 WHERE id = ?', [user.id]);
                ws.send(JSON.stringify({ type: 'login_success', user }));
                broadcastUserStatus(user.id, true);
              }
            });
          }
          break;
          
        case 'message':
          const sender = userSockets.get(ws);
          if (sender) {
            db.run('INSERT INTO messages (sender_id, receiver_id, message, type) VALUES (?, ?, ?, ?)',
              [sender.id, msg.receiver_id, msg.message, msg.msg_type || 'text'],
              function() {
                console.log(`Сообщение от ${sender.id} к ${msg.receiver_id}: ${msg.message}`);
                
                db.get('SELECT * FROM chats WHERE (user1_id = ? AND user2_id = ?) OR (user1_id = ? AND user2_id = ?)',
                  [sender.id, msg.receiver_id, msg.receiver_id, sender.id],
                  (err, chat) => {
                    if (chat) {
                      db.run('UPDATE chats SET last_message = ?, last_message_time = CURRENT_TIMESTAMP WHERE id = ?', [msg.message, chat.id]);
                    } else {
                      db.run('INSERT INTO chats (user1_id, user2_id, last_message, last_message_time) VALUES (?, ?, ?, CURRENT_TIMESTAMP)', [sender.id, msg.receiver_id, msg.message]);
                    }
                  });
                
                const receiverWs = clients.get(msg.receiver_id);
                if (receiverWs) {
                  receiverWs.send(JSON.stringify({
                    type: 'new_message',
                    message: {
                      id: this.lastID,
                      sender_id: sender.id,
                      message: msg.message,
                      type: msg.msg_type || 'text',
                      timestamp: new Date().toISOString()
                    }
                  }));
                }
              });
          }
          break;
          
        case 'get_messages':
          const user = userSockets.get(ws);
          if (user) {
            db.all(
              `SELECT * FROM messages WHERE (sender_id = ? AND receiver_id = ?) OR (sender_id = ? AND receiver_id = ?) ORDER BY timestamp ASC`,
              [user.id, msg.receiver_id, msg.receiver_id, user.id],
              (err, messages) => {
                ws.send(JSON.stringify({ type: 'messages_history', messages }));
              });
          }
          break;
          
        case 'update_profile':
          const currentUser = userSockets.get(ws);
          if (currentUser) {
            db.run('UPDATE users SET username = ?, bio = ? WHERE id = ?',
              [msg.username, msg.bio, currentUser.id],
              function() {
                ws.send(JSON.stringify({ type: 'profile_updated' }));
              });
          }
          break;
      }
    } catch (e) {
      console.error('Ошибка:', e);
    }
  });
  
  ws.on('close', () => {
    const user = userSockets.get(ws);
    if (user) {
      clients.delete(user.id);
      userSockets.delete(ws);
      db.run('UPDATE users SET is_online = 0 WHERE id = ?', [user.id]);
      broadcastUserStatus(user.id, false);
    }
  });
});

function broadcastUserStatus(userId, isOnline) {
  for (let [client, user] of userSockets) {
    client.send(JSON.stringify({
      type: 'user_status',
      user_id: userId,
      is_online: isOnline
    }));
  }
}

// API
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', users: clients.size });
});

app.post('/api/login', (req, res) => {
  const { phone } = req.body;
  db.get('SELECT * FROM users WHERE phone = ?', [phone], (err, user) => {
    if (user) {
      db.run('UPDATE users SET is_online = 1 WHERE id = ?', [user.id]);
      res.json(user);
    } else {
      db.run('INSERT INTO users (phone, is_online) VALUES (?, 1)', [phone], function() {
        res.json({ id: this.lastID, phone, username: null, bio: null, is_online: 1 });
      });
    }
  });
});

app.get('/api/users', (req, res) => {
  db.all('SELECT id, phone, username, bio, is_online FROM users', (err, users) => {
    res.json(users || []);
  });
});

app.get('/api/messages', (req, res) => {
  const { user1, user2 } = req.query;
  db.all('SELECT * FROM messages WHERE (sender_id = ? AND receiver_id = ?) OR (sender_id = ? AND receiver_id = ?) ORDER BY timestamp ASC',
    [user1, user2, user2, user1], (err, messages) => {
      res.json(messages || []);
    });
});

app.post('/api/send_message', (req, res) => {
  const { sender_id, receiver_id, message, msg_type } = req.body;
  db.run('INSERT INTO messages (sender_id, receiver_id, message, type) VALUES (?, ?, ?, ?)',
    [sender_id, receiver_id, message, msg_type || 'text'],
    function() {
      db.get('SELECT * FROM chats WHERE (user1_id = ? AND user2_id = ?) OR (user1_id = ? AND user2_id = ?)',
        [sender_id, receiver_id, receiver_id, sender_id],
        (err, chat) => {
          if (chat) {
            db.run('UPDATE chats SET last_message = ?, last_message_time = CURRENT_TIMESTAMP WHERE id = ?', [message, chat.id]);
          } else {
            db.run('INSERT INTO chats (user1_id, user2_id, last_message, last_message_time) VALUES (?, ?, ?, CURRENT_TIMESTAMP)', [sender_id, receiver_id, message]);
          }
        });
      
      const receiverWs = clients.get(receiver_id);
      if (receiverWs) {
        receiverWs.send(JSON.stringify({
          type: 'new_message',
          message: { id: this.lastID, sender_id, message, type: msg_type || 'text', timestamp: new Date().toISOString() }
        }));
      }
      res.json({ success: true, message_id: this.lastID });
    });
});

app.get('/api/chats/:user_id', (req, res) => {
  const userId = req.params.user_id;
  db.all(
    `SELECT c.*, 
            u.username as other_user_name, 
            u.phone as other_user_phone,
            u.is_online as other_user_online,
            CASE WHEN c.user1_id = ? THEN c.user2_id ELSE c.user1_id END as other_user_id
     FROM chats c 
     LEFT JOIN users u ON (u.id = CASE WHEN c.user1_id = ? THEN c.user2_id ELSE c.user1_id END) 
     WHERE c.user1_id = ? OR c.user2_id = ? 
     ORDER BY c.last_message_time DESC`,
    [userId, userId, userId, userId],
    (err, chats) => {
      res.json(chats || []);
    });
});

app.post('/api/update_profile', (req, res) => {
  const { user_id, username, bio } = req.body;
  db.run('UPDATE users SET username = ?, bio = ? WHERE id = ?', [username, bio, user_id], function() {
    res.json({ success: true });
  });
});

app.post('/api/logout', (req, res) => {
  const { user_id } = req.body;
  db.run('UPDATE users SET is_online = 0 WHERE id = ?', [user_id], function() {
    res.json({ success: true });
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`KGram сервер запущен на порту ${PORT}`);
});