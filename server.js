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

const dbPath = path.join(__dirname, 'kgram.db');
const db = new sqlite3.Database(dbPath);

db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    phone TEXT UNIQUE,
    username TEXT,
    bio TEXT,
    avatar TEXT,
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
          const userId = msg.user_id;
          if (userId) {
            db.get('SELECT * FROM users WHERE id = ?', [userId], (err, user) => {
              if (user) {
                userSockets.set(ws, user);
                clients.set(user.id, ws);
                ws.send(JSON.stringify({ type: 'login_success', user }));
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
                db.get('SELECT * FROM chats WHERE (user1_id = ? AND user2_id = ?) OR (user1_id = ? AND user2_id = ?)',
                  [sender.id, msg.receiver_id, msg.receiver_id, sender.id],
                  (err, chat) => {
                    if (chat) {
                      db.run('UPDATE chats SET last_message = ?, last_message_time = CURRENT_TIMESTAMP WHERE id = ?',
                        [msg.message, chat.id]);
                    } else {
                      db.run('INSERT INTO chats (user1_id, user2_id, last_message, last_message_time) VALUES (?, ?, ?, CURRENT_TIMESTAMP)',
                        [sender.id, msg.receiver_id, msg.message]);
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
              `SELECT m.*, u.username as sender_name 
               FROM messages m 
               LEFT JOIN users u ON m.sender_id = u.id 
               WHERE (m.sender_id = ? AND m.receiver_id = ?) OR (m.sender_id = ? AND m.receiver_id = ?) 
               ORDER BY m.timestamp ASC`,
              [user.id, msg.receiver_id, msg.receiver_id, user.id],
              (err, messages) => {
                ws.send(JSON.stringify({ type: 'messages_history', messages }));
              });
          }
          break;
          
        case 'get_chats':
          const currentUser = userSockets.get(ws);
          if (currentUser) {
            db.all(
              `SELECT c.*, u.username as other_user_name, u.phone as other_user_phone 
               FROM chats c 
               LEFT JOIN users u ON (u.id = CASE WHEN c.user1_id = ? THEN c.user2_id ELSE c.user1_id END) 
               WHERE c.user1_id = ? OR c.user2_id = ? 
               ORDER BY c.last_message_time DESC`,
              [currentUser.id, currentUser.id, currentUser.id],
              (err, chats) => {
                ws.send(JSON.stringify({ type: 'chats_list', chats }));
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
    }
  });
});

app.get('/', (req, res) => {
  res.json({ status: 'KGram server is running' });
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', users: clients.size });
});

app.post('/api/login', (req, res) => {
  const { phone } = req.body;
  db.get('SELECT * FROM users WHERE phone = ?', [phone], (err, user) => {
    if (user) {
      res.json({ id: user.id, phone: user.phone, username: user.username, bio: user.bio });
    } else {
      db.run('INSERT INTO users (phone) VALUES (?)', [phone], function() {
        res.json({ id: this.lastID, phone, username: null, bio: null });
      });
    }
  });
});

app.get('/api/users', (req, res) => {
  db.all('SELECT id, phone, username, bio FROM users', (err, users) => {
    res.json(users || []);
  });
});

app.get('/api/messages', (req, res) => {
  const { user1, user2 } = req.query;
  db.all(
    'SELECT * FROM messages WHERE (sender_id = ? AND receiver_id = ?) OR (sender_id = ? AND receiver_id = ?) ORDER BY timestamp ASC',
    [user1, user2, user2, user1],
    (err, messages) => {
      res.json(messages || []);
    }
  );
});

app.post('/api/update_profile', (req, res) => {
  const { user_id, username, bio } = req.body;
  db.run('UPDATE users SET username = ?, bio = ? WHERE id = ?', [username, bio, user_id], function() {
    res.json({ success: true });
  });
});

app.get('/api/chats/:user_id', (req, res) => {
  const userId = req.params.user_id;
  db.all(
    `SELECT c.*, u.username as other_user_name, u.phone as other_user_phone 
     FROM chats c 
     LEFT JOIN users u ON (u.id = CASE WHEN c.user1_id = ? THEN c.user2_id ELSE c.user1_id END) 
     WHERE c.user1_id = ? OR c.user2_id = ? 
     ORDER BY c.last_message_time DESC`,
    [userId, userId, userId],
    (err, chats) => {
      res.json(chats || []);
    }
  );
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`KGram сервер запущен на порту ${PORT}`);
});