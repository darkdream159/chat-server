const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;
const HOST = process.env.HOST || '0.0.0.0';
const UPLOAD_DIR = path.join(__dirname, 'uploads');

if (!fs.existsSync(UPLOAD_DIR)) {
  fs.mkdirSync(UPLOAD_DIR, { recursive: true });
}

// 内存中的用户表
const users = new Map();
// 内存中的连接表（clientId -> userId）
const connections = new Map();
const clients = new Map();

const server = http.createServer((req, res) => {
  const getBaseUrl = () => {
    const host = req.headers.host;
    const protocol = (req.headers['x-forwarded-proto'] || 'https');
    return `${protocol}://${host}`;
  };
  
  if (req.url === '/upload' && req.method === 'POST') {
    const contentType = req.headers['content-type'] || '';
    
    if (contentType.includes('multipart/form-data')) {
      const boundary = contentType.split('boundary=')[1];
      if (!boundary) {
        res.writeHead(400);
        return res.end(JSON.stringify({ error: 'No boundary' }));
      }

      const chunks = [];
      req.on('data', chunk => chunks.push(chunk));
      req.on('end', () => {
        const buffer = Buffer.concat(chunks);
        const parts = buffer.toString('binary').split(`--${boundary}`);
        
        let fileData = null;
        let fileName = 'file';
        let fieldName = 'file';

        for (const part of parts) {
          if (part.includes('Content-Disposition')) {
            const headerEnd = part.indexOf('\r\n\r\n');
            if (headerEnd === -1) continue;
            
            const headers = part.substring(0, headerEnd);
            const content = part.substring(headerEnd + 4);
            
            const nameMatch = headers.match(/name="([^"]+)"/);
            if (nameMatch) fieldName = nameMatch[1];
            
            const filenameMatch = headers.match(/filename="([^"]+)"/);
            if (filenameMatch) {
              fileName = filenameMatch[1];
              fileData = content;
            }
          }
        }

        if (fileData) {
          const ext = path.extname(fileName) || '.bin';
          const baseName = path.basename(fileName, ext);
          const safeName = `${baseName}_${Date.now()}${ext}`;
          const filePath = path.join(UPLOAD_DIR, safeName);
          
          fs.writeFileSync(filePath, fileData, 'binary');
          
          const baseUrl = getBaseUrl();
          const fileUrl = `${baseUrl}/uploads/${safeName}`;
          
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ url: fileUrl, fileName: fileName }));
        } else {
          res.writeHead(400);
          res.end(JSON.stringify({ error: 'No file data' }));
        }
      });
    } else {
      res.writeHead(400);
      res.end(JSON.stringify({ error: 'Unsupported content type' }));
    }
  } else if (req.url.startsWith('/uploads/') && req.method === 'GET') {
    const fileName = path.basename(req.url);
    const filePath = path.join(UPLOAD_DIR, fileName);
    
    if (fs.existsSync(filePath)) {
      res.writeHead(200, { 'Content-Type': 'application/octet-stream' });
      fs.createReadStream(filePath).pipe(res);
    } else {
      res.writeHead(404);
      res.end(JSON.stringify({ error: 'File not found' }));
    }
  } else {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', message: 'WebSocket server is running' }));
  }
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  let currentUserId = null;
  let currentUserInfo = null;
  const clientId = Math.random().toString(36).substr(2, 9);
  
  console.log(`新连接 ${clientId}`);
  
  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data);
      
      // 处理登录/身份验证
      if (message.type === 'login') {
        const { userId, nickname, avatar } = message;
        
        if (!userId) {
          ws.send(JSON.stringify({
            type: 'error',
            content: '缺少用户ID'
          }));
          return;
        }
        
        currentUserId = userId;
        currentUserInfo = { 
          userId, 
          nickname: nickname || `用户${userId.substr(-6)}`, 
          avatar: avatar || '' 
        };
        
        // 保存或更新用户信息
        users.set(userId, currentUserInfo);
        // 保存连接
        connections.set(clientId, userId);
        clients.set(clientId, ws);
        
        // 计算在线人数（去重）
        const onlineUserIds = new Set(connections.values());
        
        console.log(`用户 ${userId}(${currentUserInfo.nickname}) 登录，在线: ${onlineUserIds.size}人`);
        
        // 发送欢迎
        ws.send(JSON.stringify({
          type: 'welcome',
          userId: userId,
          nickname: currentUserInfo.nickname,
          avatar: currentUserInfo.avatar,
          onlineCount: onlineUserIds.size
        }));
        
        return;
      }
      
      // 普通消息
      if (!currentUserId) {
        ws.send(JSON.stringify({
          type: 'error',
          content: '请先登录'
        }));
        return;
      }
      
      // 附上用户信息
      message.from = currentUserId;
      message.userInfo = currentUserInfo;
      message.timestamp = Date.now();
      
      console.log(`收到消息 from ${currentUserInfo?.nickname || currentUserId}:`, message.type);
      
      // 广播给所有人
      clients.forEach((client, cid) => {
        if (cid !== clientId && client.readyState === WebSocket.OPEN) {
          client.send(JSON.stringify(message));
        }
      });
    } catch (error) {
      console.error('消息处理错误:', error);
    }
  });

  ws.on('close', () => {
    if (currentUserId) {
      connections.delete(clientId);
      clients.delete(clientId);
      
      // 计算在线人数
      const onlineUserIds = new Set(connections.values());
      
      console.log(`用户 ${currentUserId} 断开，在线: ${onlineUserIds.size}人`);
      
      // 通知其他人
      if (onlineUserIds.size > 0) {
        clients.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({
              type: 'system',
              content: `👤 在线人数: ${onlineUserIds.size} 人`,
              onlineCount: onlineUserIds.size
            }));
          }
        });
      }
    }
  });

  ws.on('error', (error) => {
    console.error(`连接错误:`, error);
    if (clientId) {
      connections.delete(clientId);
      clients.delete(clientId);
    }
  });
});

server.listen(PORT, HOST, () => {
  console.log(`服务器已启动，监听 ${HOST}:${PORT}`);
  console.log(`WebSocket 服务: ws://${HOST}:${PORT}`);
  console.log(`上传接口: http://${HOST}:${PORT}/upload`);
  console.log(`文件访问: http://${HOST}:${PORT}/uploads/`);
});
