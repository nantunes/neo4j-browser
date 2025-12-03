const express = require('express')
const path = require('path')
const http = require('http')
const WebSocket = require('ws')
const net = require('net')
const url = require('url')

const app = express()
const server = http.createServer(app)
const wss = new WebSocket.Server({ server })

// Serve static files from the dist directory
const distPath = path.join(__dirname, '../dist')
app.use(express.static(distPath))

// Fallback to index.html for SPA routing
app.use((req, res) => {
  res.sendFile(path.join(distPath, 'index.html'))
})

// WebSocket Proxy Logic
wss.on('connection', (ws, req) => {
  const parameters = url.parse(req.url, true)
  const target = parameters.query.target

  if (!target) {
    console.log(
      `[${new Date().toISOString()}] No target specified, closing connection`
    )
    ws.close()
    return
  }

  const [host, port] = target.split(':')
  console.log(
    `[${new Date().toISOString()}] New connection request to ${host}:${port}`
  )

  let tcpSocket = null

  try {
    tcpSocket = net.createConnection(
      { host, port: parseInt(port) || 5432 },
      () => {
        console.log(
          `[${new Date().toISOString()}] Connected to target ${host}:${port}`
        )
      }
    )
  } catch (err) {
    console.error(
      `[${new Date().toISOString()}] Failed to create TCP connection:`,
      err
    )
    ws.close()
    return
  }

  tcpSocket.on('data', data => {
    if (ws.readyState === WebSocket.OPEN) {
      try {
        ws.send(data)
      } catch (err) {
        console.error(
          `[${new Date().toISOString()}] Error sending data to WebSocket:`,
          err
        )
        tcpSocket.end()
      }
    }
  })

  tcpSocket.on('end', () => {
    console.log(`[${new Date().toISOString()}] Target disconnected`)
    ws.close()
  })

  tcpSocket.on('error', err => {
    console.error(`[${new Date().toISOString()}] Target TCP error:`, err)
    ws.close()
  })

  ws.on('message', data => {
    if (tcpSocket && !tcpSocket.destroyed) {
      try {
        tcpSocket.write(data)
      } catch (err) {
        console.error(
          `[${new Date().toISOString()}] Error writing to TCP socket:`,
          err
        )
        ws.close()
      }
    }
  })

  ws.on('close', () => {
    console.log(`[${new Date().toISOString()}] Client WebSocket disconnected`)
    if (tcpSocket) tcpSocket.end()
  })

  ws.on('error', err => {
    console.error(`[${new Date().toISOString()}] Client WebSocket error:`, err)
    if (tcpSocket) tcpSocket.end()
  })
})

const PORT = 8080
server.listen(PORT, () => {
  console.log(`[${new Date().toISOString()}] Server listening on port ${PORT}`)
})
