const WebSocket = require('ws')
const net = require('net')
const http = require('http')
const url = require('url')

// Global error handlers to prevent exit
process.on('uncaughtException', err => {
  console.error(`[${new Date().toISOString()}] Uncaught Exception:`, err)
})

process.on('unhandledRejection', (reason, promise) => {
  console.error(
    `[${new Date().toISOString()}] Unhandled Rejection at:`,
    promise,
    'reason:',
    reason
  )
})

const server = http.createServer()
const wss = new WebSocket.Server({ server })

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

const PORT = 8081
server.listen(PORT, () => {
  console.log(
    `[${new Date().toISOString()}] WebSocket proxy listening on port ${PORT}`
  )
})
