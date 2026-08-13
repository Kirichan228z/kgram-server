const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = 'https://znlvqynabxortgfnruwb.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpubHZxeW5hYnhvcnRnZm5ydXdiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODY2MzIwOTcsImV4cCI6MjEwMjIwODA5N30.1eHarKE1SXCI-wdNYLzFSJYf38HcMaAUPie1ZFEDoDQ';
const supabase = createClient(supabaseUrl, supabaseKey);

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const clients = new Map();
const userSockets = new Map();

wss.on('connection', (ws) => {
  console.log('Новое подключение');
  
  ws.on('message', async (data) => {
    try {
      const msg = JSON.parse(data);
      
      switch(msg.type) {
        case 'login':
          if (msg.user_id) {
            const { data: user } = await supabase.from('users').select('*').eq('id', msg.user_id).single();
            if (user) {
              userSockets.set(ws, user);
              clients.set(user.id, ws);
              await supabase.from('users').update({ is_online: 1 }).eq('id', user.id);
              ws.send(JSON.stringify({ type: 'login_success', user }));
            }
          }
          break;
          
        case 'message':
          const sender = userSockets.get(ws);
          if (sender) {
            const { data: newMsg } = await supabase.from('messages').insert({
              sender_id: sender.id,
              receiver_id: msg.receiver_id,
              message: msg.message,
              type: msg.msg_type || 'text'
            }).select().single();
            
            const receiverWs = clients.get(msg.receiver_id);
            if (receiverWs) {
              receiverWs.send(JSON.stringify({
                type: 'new_message',
                message: { ...newMsg, timestamp: new Date().toISOString() }
              }));
            }
          }
          break;
      }
    } catch (e) {
      console.error('Ошибка WebSocket:', e);
    }
  });
  
  ws.on('close', async () => {
    const user = userSockets.get(ws);
    if (user) {
      clients.delete(user.id);
      userSockets.delete(ws);
      await supabase.from('users').update({ is_online: 0 }).eq('id', user.id);
    }
  });
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.post('/api/login', async (req, res) => {
  const { phone } = req.body;
  const { data: existingUser } = await supabase.from('users').select('*').eq('phone', phone).single();
  if (existingUser) {
    await supabase.from('users').update({ is_online: 1 }).eq('id', existingUser.id);
    res.json(existingUser);
  } else {
    const { data: newUser } = await supabase.from('users').insert({ phone, is_online: 1 }).select().single();
    res.json(newUser);
  }
});

app.get('/api/users', async (req, res) => {
  const { data: users } = await supabase.from('users').select('*');
  res.json(users || []);
});

app.get('/api/messages', async (req, res) => {
  const { user1, user2 } = req.query;
  const { data: messages } = await supabase.from('messages')
    .select('*')
    .or(`and(sender_id.eq.${user1},receiver_id.eq.${user2}),and(sender_id.eq.${user2},receiver_id.eq.${user1})`)
    .order('timestamp', { ascending: true });
  res.json(messages || []);
});

app.post('/api/send_message', async (req, res) => {
  const { sender_id, receiver_id, message, msg_type } = req.body;
  const { data: newMsg } = await supabase.from('messages').insert({
    sender_id, receiver_id, message, type: msg_type || 'text'
  }).select().single();
  
  const { data: existingChat } = await supabase.from('chats')
    .select('*')
    .or(`and(user1_id.eq.${sender_id},user2_id.eq.${receiver_id}),and(user1_id.eq.${receiver_id},user2_id.eq.${sender_id})`)
    .single();
  
  if (existingChat) {
    await supabase.from('chats').update({ last_message: message, last_message_time: new Date().toISOString() }).eq('id', existingChat.id);
  } else {
    await supabase.from('chats').insert({ user1_id: sender_id, user2_id: receiver_id, last_message: message, last_message_time: new Date().toISOString() });
  }
  
  res.json({ success: true });
});

app.get('/api/chats/:user_id', async (req, res) => {
  const userId = req.params.user_id;
  const { data: chats } = await supabase.from('chats')
    .select('*')
    .or(`user1_id.eq.${userId},user2_id.eq.${userId}`)
    .order('last_message_time', { ascending: false });
  
  const formattedChats = [];
  for (const chat of (chats || [])) {
    const otherId = chat.user1_id == userId ? chat.user2_id : chat.user1_id;
    const { data: otherUser } = await supabase.from('users').select('*').eq('id', otherId).single();
    formattedChats.push({
      ...chat,
      other_user_id: otherId,
      other_user_name: otherUser?.username || null,
      other_user_phone: otherUser?.phone || 'Пользователь',
      other_user_online: otherUser?.is_online || 0
    });
  }
  res.json(formattedChats);
});

app.post('/api/update_profile', async (req, res) => {
  const { user_id, username, bio } = req.body;
  await supabase.from('users').update({ username, bio }).eq('id', user_id);
  res.json({ success: true });
});

app.post('/api/logout', async (req, res) => {
  const { user_id } = req.body;
  await supabase.from('users').update({ is_online: 0 }).eq('id', user_id);
  res.json({ success: true });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`KGram сервер запущен на порту ${PORT}`);
});
