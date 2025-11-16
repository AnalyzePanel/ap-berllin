//+------------------------------------------------------------------+
//|                                          DataPusher_Optimized.mq5 |
//|                                   Optimized for Marathon RabbitMQ |
//|                                  Implements optimization strategy |
//+------------------------------------------------------------------+
#include "/socketlib.mqh"
#include <Trade/Trade.mqh>
#include <JAson.mqh>

// Additional socket functions for keepalive
#import "Ws2_32.dll"
int setsockopt(SOCKET64 s, int level, int optname, int &optval, int optlen);
#import

// Socket option constants
#define SOL_SOCKET 0xFFFF
#define SO_KEEPALIVE 0x0008
#define MSG_PEEK 0x02

// Connection settings
#define SERVER_IP_ADDRESS "127.0.0.1"
#define SERVER_PORT 8888

// Expert status constants
#define STATUS_DISCONNECTED   0
#define STATUS_CONNECTING     1
#define STATUS_CONNECTED      2
#define STATUS_ERROR         -1
#define STATUS_RECONNECTING  -2

// Message intervals (in seconds)
#define SNAPSHOT_INTERVAL 30  // Full account snapshot every 30 seconds
#define UPDATE_INTERVAL 5     // Quick updates every 5 seconds
#define HEARTBEAT_INTERVAL 8  // Send heartbeat every 8 seconds to keep connection alive (more aggressive)

// Global variables  
SOCKET64 client = INVALID_SOCKET64;
int expertStatus = STATUS_DISCONNECTED;
datetime lastConnectionAttempt = 0;
int reconnectDelay = 5;
CTrade trade;

// Tracking variables
double lastEquity = 0;
double lastBalance = 0;
double lastProfit = 0;
int lastPositionsTotal = 0;
int lastOrdersTotal = 0;
datetime lastSnapshotSent = 0;
datetime lastUpdateSent = 0;
datetime lastDataSent = 0;  // Track last time any data was sent
string lastPositionsHash = "";
string lastOrdersHash = "";

// Incoming data buffer
string recvBuffer = "";

// Forward declarations
void SendAccountSnapshot();
void SendQuickUpdate();
void SendPositionsUpdate();
void SendOrdersUpdate();
string GetCurrentTimestamp();
void ProcessSocketError(string functionName, int errorCode);
void SendData(string data);
void PollIncoming();
bool TryReceiveLine(string &lineOut);
void HandleIncomingMessage(const string message);
void HandleQuery(CJAVal &query);
bool HasEquityChanged();
bool HasPositionsChanged();
bool HasOrdersChanged();
string CalculatePositionsHash();
string CalculateOrdersHash();
void SendHeartbeat();

//+------------------------------------------------------------------+
//| Expert initialization function                                     |
//+------------------------------------------------------------------+
int OnInit() {
   expertStatus = STATUS_CONNECTING;
   ConnectSocket();
   
   // Initialize tracking variables
   lastEquity = AccountInfoDouble(ACCOUNT_EQUITY);
   lastBalance = AccountInfoDouble(ACCOUNT_BALANCE);
   lastProfit = AccountInfoDouble(ACCOUNT_PROFIT);
   lastPositionsTotal = PositionsTotal();
   lastOrdersTotal = OrdersTotal();
   lastSnapshotSent = TimeCurrent();
   lastUpdateSent = TimeCurrent();
   lastDataSent = TimeCurrent();
   lastPositionsHash = CalculatePositionsHash();
   lastOrdersHash = CalculateOrdersHash();
   
   // Send initial full snapshot
   if(IsSocketConnected()) {
      SendAccountSnapshot();
   }
   
   Print("✅ DataPusher Optimized initialized with Marathon strategy");
   
   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                   |
//+------------------------------------------------------------------+
void OnDeinit(const int reason) {
   CloseClean();
}

//+------------------------------------------------------------------+
//| Expert tick function                                               |
//+------------------------------------------------------------------+
void OnTick() {
   // Handle reconnection if needed
   if(expertStatus == STATUS_DISCONNECTED || expertStatus == STATUS_ERROR || expertStatus == STATUS_RECONNECTING) {
      datetime now = TimeCurrent();
      int secondsSinceLastAttempt = (int)(now - lastConnectionAttempt);
      
      // Only attempt reconnection if enough time has passed
      if(secondsSinceLastAttempt >= reconnectDelay) {
         Print("Attempting to reconnect... (", secondsSinceLastAttempt, "s since last attempt)");
         expertStatus = STATUS_RECONNECTING;
         ConnectSocket();
         
         // After connection attempt, check if we're connected
         if(expertStatus == STATUS_CONNECTED && IsSocketConnected()) {
            Print("✅ Reconnection successful, sending snapshot...");
            // Send full snapshot after reconnection
            SendAccountSnapshot();
         } else {
            // Connection failed, reset for next attempt
            Print("⚠️ Reconnection attempt failed, will retry in ", reconnectDelay, " seconds");
            expertStatus = STATUS_DISCONNECTED;
            lastConnectionAttempt = TimeCurrent();
         }
      }
      return;
   }
   
   // Verify connection is still alive
   if(!IsSocketConnected()) {
      expertStatus = STATUS_DISCONNECTED;
      lastConnectionAttempt = TimeCurrent();
      return;
   }
   
   datetime now = TimeCurrent();
   
   // Note: Removed aggressive health check using recv() as it was causing disconnections
   // Connection health will be detected by send() errors instead
   
   // -1. Poll for incoming server queries (non-blocking)
   PollIncoming();
   
   // 0. Send heartbeat if idle too long (prevents connection timeout)
   if(int(now - lastDataSent) >= HEARTBEAT_INTERVAL) {
      SendHeartbeat();
      return; // One message per tick
   }
   
   // 1. Check if we need to send full snapshot (every 30 seconds)
   if(int(now - lastSnapshotSent) >= SNAPSHOT_INTERVAL) {
      SendAccountSnapshot();
      lastSnapshotSent = now;
      lastUpdateSent = now; // Reset update timer after snapshot
      return; // Skip other checks this tick
   }
   
   // 2. Check for position changes (immediate + periodic)
   if(HasPositionsChanged()) {
      SendPositionsUpdate();
      lastPositionsTotal = PositionsTotal();
      lastPositionsHash = CalculatePositionsHash();
      return; // One message per tick
   }
   
   // 3. Check for order changes (immediate + periodic)
   if(HasOrdersChanged()) {
      SendOrdersUpdate();
      lastOrdersTotal = OrdersTotal();
      lastOrdersHash = CalculateOrdersHash();
      return; // One message per tick
   }
   
   // 4. Check for equity changes (send quick update every 5 seconds)
   // Send update even if equity hasn't changed to keep connection alive
   if(int(now - lastUpdateSent) >= UPDATE_INTERVAL) {
      SendQuickUpdate();
      lastUpdateSent = now;
   }
}

//+------------------------------------------------------------------+
//| Socket connection function                                         |
//+------------------------------------------------------------------+
void ConnectSocket() {
   // Always close existing socket before reconnecting
   if(client != INVALID_SOCKET64) {
      closesocket(client);
      client = INVALID_SOCKET64;
   }
   
   lastConnectionAttempt = TimeCurrent();
   
   char wsaData[]; ArrayResize(wsaData,sizeof(WSAData));
   int res = WSAStartup(MAKEWORD(2,2),wsaData);
   if(res != 0) {
      Print("⚠️ WSAStartup failed: ", res);
      expertStatus = STATUS_ERROR;
      return;
   }

   client = socket(AF_INET,SOCK_STREAM,IPPROTO_TCP);
   if(client == INVALID_SOCKET64) {
      int err = WSAGetLastError();
      Print("⚠️ Socket creation failed: ", WSAErrorDescript(err));
      WSACleanup();
      expertStatus = STATUS_ERROR;
      return;
   }

   char ch[];
   StringToCharArray(SERVER_IP_ADDRESS,ch);
   sockaddr_in addrin;
   addrin.sin_family = AF_INET;
   addrin.sin_addr = inet_addr(ch);
   addrin.sin_port = htons(SERVER_PORT);
   ref_sockaddr ref;
   sockaddrIn2RefSockaddr(addrin,ref);

   res = connect(client,ref.ref,sizeof(addrin));
   if(res == SOCKET_ERROR) {
      int err = WSAGetLastError();
      if(err != WSAEISCONN && err != WSAEWOULDBLOCK && err != WSAEINPROGRESS) {
         Print("⚠️ Connection failed: ", WSAErrorDescript(err));
         closesocket(client);
         client = INVALID_SOCKET64;
         WSACleanup();
         expertStatus = STATUS_ERROR;
         return;
      }
      // WSAEWOULDBLOCK or WSAEINPROGRESS is OK for non-blocking socket
   }

   int non_block=1;
   res = ioctlsocket(client,(int)FIONBIO,non_block);
   if(res != NO_ERROR) {
      Print("⚠️ ioctlsocket failed: ", res);
      closesocket(client);
      client = INVALID_SOCKET64;
      WSACleanup();
      expertStatus = STATUS_ERROR;
      return;
   }

   // Enable TCP keepalive to prevent idle connection timeout
   int keepalive = 1;
   setsockopt(client, SOL_SOCKET, SO_KEEPALIVE, keepalive, sizeof(int));

   expertStatus = STATUS_CONNECTED;
   lastDataSent = TimeCurrent();
   Print(__FUNCTION__," > socket created and connected successfully");
}

//+------------------------------------------------------------------+
//| Clean socket closure function                                      |
//+------------------------------------------------------------------+
void CloseClean() {
   if(client != INVALID_SOCKET64) {
      // Shutdown socket gracefully before closing
      shutdown(client, 2); // 2 = SD_BOTH (shutdown both send and receive)
      closesocket(client);
      client = INVALID_SOCKET64;
   }
   WSACleanup();
   expertStatus = STATUS_DISCONNECTED;
   Print(__FUNCTION__," > socket closed...");
}

//+------------------------------------------------------------------+
//| Send full account snapshot (type: account)                        |
//+------------------------------------------------------------------+
void SendAccountSnapshot() {
   CJAVal data;
   
   data["type"] = "account";
   data["login"] = (string)AccountInfoInteger(ACCOUNT_LOGIN);  // ✅ STRING
   data["balance"] = AccountInfoDouble(ACCOUNT_BALANCE);
   data["equity"] = AccountInfoDouble(ACCOUNT_EQUITY);
   data["profit"] = AccountInfoDouble(ACCOUNT_PROFIT);
   data["margin"] = AccountInfoDouble(ACCOUNT_MARGIN);
   data["freeMargin"] = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
   data["currency"] = AccountInfoString(ACCOUNT_CURRENCY);
   data["leverage"] = (int)AccountInfoInteger(ACCOUNT_LEVERAGE);
   data["timestamp"] = GetCurrentTimestamp();  // ✅ TIMESTAMP
   
   // Add positions
   data["positions"];
   for(int i = 0; i < PositionsTotal(); i++) {
      ulong ticket = PositionGetTicket(i);
      if(ticket <= 0) continue;
      
      CJAVal position;
      position["ticket"] = (long)ticket;
      position["symbol"] = PositionGetString(POSITION_SYMBOL);
      position["type"] = EnumToString((ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE));
      position["volume"] = PositionGetDouble(POSITION_VOLUME);
      position["openPrice"] = PositionGetDouble(POSITION_PRICE_OPEN);
      position["currentPrice"] = PositionGetDouble(POSITION_PRICE_CURRENT);
      position["sl"] = PositionGetDouble(POSITION_SL);
      position["tp"] = PositionGetDouble(POSITION_TP);
      position["profit"] = PositionGetDouble(POSITION_PROFIT);
      position["swap"] = PositionGetDouble(POSITION_SWAP);
      position["commission"] = 0.0; // MT5 doesn't expose this easily
      
      data["positions"].Add(position);
   }
   
   // Add orders
   data["orders"];
   for(int i = 0; i < OrdersTotal(); i++) {
      ulong ticket = OrderGetTicket(i);
      if(ticket <= 0) continue;
      
      CJAVal order;
      order["ticket"] = (long)ticket;
      order["symbol"] = OrderGetString(ORDER_SYMBOL);
      order["type"] = EnumToString((ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE));
      order["volume"] = OrderGetDouble(ORDER_VOLUME_INITIAL);
      order["price"] = OrderGetDouble(ORDER_PRICE_OPEN);
      order["currentPrice"] = OrderGetDouble(ORDER_PRICE_CURRENT);
      
      data["orders"].Add(order);
   }
   
   SendData(data.Serialize());
   
   // Update tracking
   lastEquity = AccountInfoDouble(ACCOUNT_EQUITY);
   lastBalance = AccountInfoDouble(ACCOUNT_BALANCE);
   lastProfit = AccountInfoDouble(ACCOUNT_PROFIT);
   
   Print("📸 Sent full account snapshot");
}

//+------------------------------------------------------------------+
//| Send quick update (type: update)                                  |
//+------------------------------------------------------------------+
void SendQuickUpdate() {
   CJAVal data;
   
   data["type"] = "update";
   data["login"] = (string)AccountInfoInteger(ACCOUNT_LOGIN);  // ✅ STRING
   data["equity"] = AccountInfoDouble(ACCOUNT_EQUITY);
   data["profit"] = AccountInfoDouble(ACCOUNT_PROFIT);
   data["freeMargin"] = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
   data["timestamp"] = GetCurrentTimestamp();  // ✅ TIMESTAMP
   
   // Add balance only if changed
   double currentBalance = AccountInfoDouble(ACCOUNT_BALANCE);
   if(MathAbs(currentBalance - lastBalance) > 0.01) {
      data["balance"] = currentBalance;
      lastBalance = currentBalance;
   }
   
   // Add margin only if it exists
   double currentMargin = AccountInfoDouble(ACCOUNT_MARGIN);
   if(currentMargin > 0.01) {
      data["margin"] = currentMargin;
   }
   
   SendData(data.Serialize());
   
   // Update tracking
   lastEquity = AccountInfoDouble(ACCOUNT_EQUITY);
   lastProfit = AccountInfoDouble(ACCOUNT_PROFIT);
   
   Print("🔄 Sent quick update");
}

//+------------------------------------------------------------------+
//| Send positions update (type: positions)                           |
//+------------------------------------------------------------------+
void SendPositionsUpdate() {
   CJAVal data;
   
   data["type"] = "positions";
   data["login"] = (string)AccountInfoInteger(ACCOUNT_LOGIN);  // ✅ STRING
   data["timestamp"] = GetCurrentTimestamp();  // ✅ TIMESTAMP
   data["positions"];
   
   for(int i = 0; i < PositionsTotal(); i++) {
      ulong ticket = PositionGetTicket(i);
      if(ticket <= 0) continue;
      
      CJAVal position;
      position["ticket"] = (long)ticket;
      position["symbol"] = PositionGetString(POSITION_SYMBOL);
      position["type"] = EnumToString((ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE));
      position["volume"] = PositionGetDouble(POSITION_VOLUME);
      position["openPrice"] = PositionGetDouble(POSITION_PRICE_OPEN);
      position["currentPrice"] = PositionGetDouble(POSITION_PRICE_CURRENT);
      position["sl"] = PositionGetDouble(POSITION_SL);
      position["tp"] = PositionGetDouble(POSITION_TP);
      position["profit"] = PositionGetDouble(POSITION_PROFIT);
      position["swap"] = PositionGetDouble(POSITION_SWAP);
      position["commission"] = 0.0;
      
      data["positions"].Add(position);
   }
   
   SendData(data.Serialize());
   Print("📊 Sent positions update (", PositionsTotal(), " positions)");
}

//+------------------------------------------------------------------+
//| Send orders update (type: orders)                                 |
//+------------------------------------------------------------------+
void SendOrdersUpdate() {
   CJAVal data;
   
   data["type"] = "orders";
   data["login"] = (string)AccountInfoInteger(ACCOUNT_LOGIN);  // ✅ STRING
   data["timestamp"] = GetCurrentTimestamp();  // ✅ TIMESTAMP
   data["orders"];
   
   for(int i = 0; i < OrdersTotal(); i++) {
      ulong ticket = OrderGetTicket(i);
      if(ticket <= 0) continue;
      
      CJAVal order;
      order["ticket"] = (long)ticket;
      order["symbol"] = OrderGetString(ORDER_SYMBOL);
      order["type"] = EnumToString((ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE));
      order["volume"] = OrderGetDouble(ORDER_VOLUME_INITIAL);
      order["price"] = OrderGetDouble(ORDER_PRICE_OPEN);
      order["currentPrice"] = OrderGetDouble(ORDER_PRICE_CURRENT);
      
      data["orders"].Add(order);
   }
   
   SendData(data.Serialize());
   Print("📋 Sent orders update (", OrdersTotal(), " orders)");
}

//+------------------------------------------------------------------+
//| Get current timestamp in ISO 8601 format                          |
//+------------------------------------------------------------------+
string GetCurrentTimestamp() {
   MqlDateTime dt;
   TimeCurrent(dt);
   
   return StringFormat(
      "%04d-%02d-%02dT%02d:%02d:%02dZ",
      dt.year, dt.mon, dt.day,
      dt.hour, dt.min, dt.sec
   );
}

//+------------------------------------------------------------------+
//| Send data function with logging                                    |
//+------------------------------------------------------------------+
void SendData(string data) {
   if(!IsSocketConnected()) return;
   
   // Note: Removed health check before send - send() will detect connection errors
   // This avoids interfering with the connection using recv()
   
   // Log the data being sent (first 200 chars to avoid spam)
   string preview = StringSubstr(data, 0, 200);
   if(StringLen(data) > 200) {
      preview += "...";
   }
   Print("📤 Sending to Tokyo: ", preview);
   
   // Add message terminator
   data = data + "\n";
   
   char dataArray[];
   StringToCharArray(data, dataArray);
   
   int bytesSent = send(client, dataArray, ArraySize(dataArray)-1, 0);
   if(bytesSent == SOCKET_ERROR) {
      int errorCode = WSAGetLastError();
      
      // WSAEWOULDBLOCK is not a fatal error in non-blocking mode
      // It just means the send buffer is full, try again later
      if(errorCode == WSAEWOULDBLOCK) {
         Print("⚠️ Send buffer full (WSAEWOULDBLOCK), will retry later");
         return;
      }
      
      // Connection abort errors - connection was closed unexpectedly
      if(errorCode == WSAECONNABORTED || errorCode == WSAECONNRESET || errorCode == WSAENOTCONN || errorCode == WSAESHUTDOWN) {
         Print("⚠️ Connection lost during send (", errorCode, "), will reconnect");
         CloseClean();
         expertStatus = STATUS_DISCONNECTED;
         lastConnectionAttempt = TimeCurrent();
         return;
      }
      
      // Only treat other errors as fatal
      ProcessSocketError(__FUNCTION__, errorCode);
   } else if(bytesSent == 0) {
      // send() returned 0 - connection was closed by peer
      Print("⚠️ Connection closed by peer (send returned 0), will reconnect");
      CloseClean();
      expertStatus = STATUS_DISCONNECTED;
      lastConnectionAttempt = TimeCurrent();
      return;
   } else {
      Print("✅ Sent ", bytesSent, " bytes successfully");
      lastDataSent = TimeCurrent(); // Update last data sent time
   }
}

//+------------------------------------------------------------------+
//| Check if equity has changed significantly                         |
//+------------------------------------------------------------------+
bool HasEquityChanged() {
   double currentEquity = AccountInfoDouble(ACCOUNT_EQUITY);
   return MathAbs(currentEquity - lastEquity) > 0.01;
}

//+------------------------------------------------------------------+
//| Check if positions have changed                                    |
//+------------------------------------------------------------------+
bool HasPositionsChanged() {
   // First check total number of positions
   if(PositionsTotal() != lastPositionsTotal) return true;
   
   // If total is same, check for changes in existing positions
   string currentHash = CalculatePositionsHash();
   return currentHash != lastPositionsHash;
}

//+------------------------------------------------------------------+
//| Check if orders have changed                                       |
//+------------------------------------------------------------------+
bool HasOrdersChanged() {
   // First check total number of orders
   if(OrdersTotal() != lastOrdersTotal) return true;
   
   // If total is same, check for changes in existing orders
   string currentHash = CalculateOrdersHash();
   return currentHash != lastOrdersHash;
}

//+------------------------------------------------------------------+
//| Calculate hash for positions                                       |
//+------------------------------------------------------------------+
string CalculatePositionsHash() {
   string hash = "";
   
   for(int i = 0; i < PositionsTotal(); i++) {
      ulong ticket = PositionGetTicket(i);
      if(ticket <= 0) continue;
      
      // Combine key position properties into a string
      hash += StringFormat("%d|%.5f|%.5f|%.5f|",
         ticket,
         PositionGetDouble(POSITION_VOLUME),
         PositionGetDouble(POSITION_SL),
         PositionGetDouble(POSITION_TP)
      );
   }
   
   return hash;
}

//+------------------------------------------------------------------+
//| Calculate hash for orders                                          |
//+------------------------------------------------------------------+
string CalculateOrdersHash() {
   string hash = "";
   
   for(int i = 0; i < OrdersTotal(); i++) {
      ulong ticket = OrderGetTicket(i);
      if(ticket <= 0) continue;
      
      // Combine key order properties into a string
      hash += StringFormat("%d|%s|%d|%.5f|%.5f|%.5f|%.5f|",
         ticket,
         OrderGetString(ORDER_SYMBOL),
         OrderGetInteger(ORDER_TYPE),
         OrderGetDouble(ORDER_VOLUME_INITIAL),
         OrderGetDouble(ORDER_PRICE_OPEN),
         OrderGetDouble(ORDER_SL),
         OrderGetDouble(ORDER_TP)
      );
   }
   
   return hash;
}

//+------------------------------------------------------------------+
//| Send heartbeat to keep connection alive                           |
//+------------------------------------------------------------------+
void SendHeartbeat() {
   CJAVal data;
   
   data["type"] = "heartbeat";
   data["login"] = (string)AccountInfoInteger(ACCOUNT_LOGIN);
   data["timestamp"] = GetCurrentTimestamp();
   
   SendData(data.Serialize());
   Print("💓 Sent heartbeat");
}

//+------------------------------------------------------------------+
//| Error handling function                                            |
//+------------------------------------------------------------------+
void ProcessSocketError(string functionName, int errorCode) {
   expertStatus = STATUS_ERROR;
   Print(functionName, " > Error: ", WSAErrorDescript(errorCode));
   CloseClean();
}

//+------------------------------------------------------------------+
//| Socket connection check function                                   |
//+------------------------------------------------------------------+
bool IsSocketConnected() {
   // Basic check - socket handle must be valid
   // Actual connection state will be detected by send() errors
   return client != INVALID_SOCKET64 && expertStatus == STATUS_CONNECTED;
}

//+------------------------------------------------------------------+
//| Receive and process incoming messages (queries)                  |
//+------------------------------------------------------------------+
void PollIncoming() {
   if(!IsSocketConnected()) return;
   
   // Try to extract complete newline-delimited messages
   string line;
   int processed = 0;
   while(processed < 5 && TryReceiveLine(line)) { // limit per tick
      if(StringLen(line) > 0) {
         HandleIncomingMessage(line);
      }
      processed++;
   }
}

// Attempt to non-blocking receive until a newline appears.
// Returns true if a line (without newline) was produced in lineOut.
bool TryReceiveLine(string &lineOut) {
   lineOut = "";
   if(!IsSocketConnected()) return false;
   
   // Read available bytes
   char buf[2048];
   int received = recv(client, buf, sizeof(buf), 0);
   if(received == SOCKET_ERROR) {
      int err = WSAGetLastError();
      // No data ready
      if(err == WSAEWOULDBLOCK) return false;
      // Connection errors
      if(err == WSAECONNABORTED || err == WSAECONNRESET || err == WSAENOTCONN || err == WSAESHUTDOWN) {
         Print("⚠️ Connection lost during recv (", err, "), will reconnect");
         CloseClean();
         expertStatus = STATUS_DISCONNECTED;
         lastConnectionAttempt = TimeCurrent();
         return false;
      }
      // Other errors
      ProcessSocketError(__FUNCTION__, err);
      return false;
   }
   
   if(received <= 0) {
      return false;
   }
   
   // Append to string buffer (convert bytes -> UTF-8 string explicitly)
   string chunk;
   uchar ubuf[];
   ArrayResize(ubuf, received);
   for(int i=0;i<received;i++) ubuf[i] = (uchar)buf[i];
   // Build string manually to skip any null bytes and avoid early termination
   // while still preserving UTF-8 bytes as-is.
   int nonzero = 0;
   for(int j=0;j<received;j++) {
      uchar b = ubuf[j];
      if(b == 0) continue; // skip nulls
      nonzero++;
      // Append raw byte; for UTF-8 this is safe as MQL5 strings are UTF-16,
      // but appending byte-by-byte preserves content for JSON ASCII.
      chunk += CharToString((ushort)b);
   }
   // Hex preview of first bytes to diagnose framing/encoding
   string hexPrev = "";
   int maxHex = (received < 32 ? received : 32);
   for(int k=0;k<maxHex;k++) {
      hexPrev += StringFormat("%02X ", ubuf[k]);
   }
   // Debug: log raw recv preview and byte count
   string rprev = StringSubstr(chunk, 0, 180);
   if(StringLen(chunk) > 180) rprev += "...";
   Print("📩 recv(", received, " bytes, nonzero=", nonzero, ") chunk=", rprev, " | hex=", hexPrev);
   recvBuffer += chunk;
   
   // Extract one line if newline present
   int nlIndex = StringFind(recvBuffer, "\n");
   if(nlIndex >= 0) {
      lineOut = StringSubstr(recvBuffer, 0, nlIndex);
      // Remove processed part including newline
      recvBuffer = StringSubstr(recvBuffer, nlIndex + 1);
      StringTrimRight(lineOut);
      StringTrimLeft(lineOut);
      return true;
   }
   
   return false;
}

//+------------------------------------------------------------------+
//| Dispatch incoming JSON message                                   |
//+------------------------------------------------------------------+
void HandleIncomingMessage(const string message) {
   // Ignore empty
   if(StringLen(message) == 0) return;
   
   // Log a short preview of any incoming line
   string _preview = StringSubstr(message, 0, 180);
   if(StringLen(message) > 180) _preview += "...";
   Print("📥 Incoming line from Tokyo: ", _preview);
   
   // Parse JSON
   CJAVal parsed;
   if(!parsed.Deserialize(message)) {
      Print("⚠️ Failed to parse incoming JSON: ", message);
      return;
   }
   
   CJAVal *typePtr = parsed["type"];
   string msgType = (typePtr != NULL ? typePtr.ToStr() : "");
   
   // Expect queries with type="query"
   if(msgType == "query") {
      // Log that a query type message is being handled
      Print("🧭 Received query message");
      HandleQuery(parsed);
      return;
   }
   
   // Unknown messages are ignored (telemetry is unidirectional from EA)
}

//+------------------------------------------------------------------+
//| Handle a query and send a response                                |
//+------------------------------------------------------------------+
void HandleQuery(CJAVal &query) {
   CJAVal *reqIdPtr = query["requestId"];
   CJAVal *actionPtr = query["action"];
   string requestId = (reqIdPtr != NULL ? reqIdPtr.ToStr() : "");
   string action = (actionPtr != NULL ? actionPtr.ToStr() : "");
   
   // Log action, requestId and lightweight params preview
   string paramsPreview = "";
   CJAVal *paramsPtr = query["params"];
   if(CheckPointer(paramsPtr) != POINTER_INVALID) {
      string raw = paramsPtr.Serialize();
      paramsPreview = StringSubstr(raw, 0, 200);
      if(StringLen(raw) > 200) paramsPreview += "...";
   }
   Print("🔎 Query received | action=", action, " | requestId=", requestId, " | params=", paramsPreview);
   
   CJAVal resp;
   resp["type"] = "response";
   resp["requestId"] = requestId;
   resp["login"] = (string)AccountInfoInteger(ACCOUNT_LOGIN);
   resp["timestamp"] = GetCurrentTimestamp();
   resp["ok"] = true;
   
   bool handled = true;
   
   if(action == "ping") {
      // Simple liveness response
      CJAVal pong;
      pong["message"] = "pong";
      pong["ea_time"] = GetCurrentTimestamp();
      resp["data"] = pong;
   } else
   if(action == "get_account") {
      // Include latest account snapshot
      CJAVal acc;
      acc["balance"] = AccountInfoDouble(ACCOUNT_BALANCE);
      acc["equity"] = AccountInfoDouble(ACCOUNT_EQUITY);
      acc["profit"] = AccountInfoDouble(ACCOUNT_PROFIT);
      acc["margin"] = AccountInfoDouble(ACCOUNT_MARGIN);
      acc["freeMargin"] = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
      acc["currency"] = AccountInfoString(ACCOUNT_CURRENCY);
      acc["leverage"] = (int)AccountInfoInteger(ACCOUNT_LEVERAGE);
      resp["data"] = acc;
   } else
   if(action == "get_positions") {
      CJAVal arr;
      for(int i=0;i<PositionsTotal();i++) {
         ulong t = PositionGetTicket(i);
         if(t<=0) continue;
         CJAVal p;
         p["ticket"] = (long)t;
         p["symbol"] = PositionGetString(POSITION_SYMBOL);
         p["type"] = EnumToString((ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE));
         p["volume"] = PositionGetDouble(POSITION_VOLUME);
         p["openPrice"] = PositionGetDouble(POSITION_PRICE_OPEN);
         p["currentPrice"] = PositionGetDouble(POSITION_PRICE_CURRENT);
         p["sl"] = PositionGetDouble(POSITION_SL);
         p["tp"] = PositionGetDouble(POSITION_TP);
         p["profit"] = PositionGetDouble(POSITION_PROFIT);
         arr.Add(p);
      }
      resp["data"] = arr;
   } else
   if(action == "get_orders") {
      CJAVal arr;
      for(int i=0;i<OrdersTotal();i++) {
         ulong t = OrderGetTicket(i);
         if(t<=0) continue;
         CJAVal o;
         o["ticket"] = (long)t;
         o["symbol"] = OrderGetString(ORDER_SYMBOL);
         o["type"] = EnumToString((ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE));
         o["volume"] = OrderGetDouble(ORDER_VOLUME_INITIAL);
         o["price"] = OrderGetDouble(ORDER_PRICE_OPEN);
         o["currentPrice"] = OrderGetDouble(ORDER_PRICE_CURRENT);
         arr.Add(o);
      }
      resp["data"] = arr;
   } else
   if(action == "get_symbols") {
      CJAVal arr;
      int total = SymbolsTotal(true);
      for(int i=0;i<total;i++) {
         string s = SymbolName(i,true);
         CJAVal it;
         it["symbol"] = s;
         arr.Add(it);
      }
      resp["data"] = arr;
   } else
   if(action == "get_symbol_info") {
      CJAVal *qparams = query["params"];
      string symbol = "";
      if(CheckPointer(qparams)!=POINTER_INVALID) {
         CJAVal *sym = (*qparams)["symbol"];
         if(CheckPointer(sym)!=POINTER_INVALID) symbol = sym.ToStr();
      }
      if(SymbolSelect(symbol,true)) {
         CJAVal si;
         si["symbol"] = symbol;
         si["digits"] = (int)SymbolInfoInteger(symbol,SYMBOL_DIGITS);
         si["point"] = SymbolInfoDouble(symbol,SYMBOL_POINT);
         si["trade_mode"] = (int)SymbolInfoInteger(symbol,SYMBOL_TRADE_MODE);
         si["spread"] = SymbolInfoInteger(symbol,SYMBOL_SPREAD);
         resp["data"] = si;
      } else {
         resp["ok"] = false;
         resp["error"] = "SymbolSelect failed";
      }
   } else
   if(action == "get_symbol_tick") {
      CJAVal *qparams2 = query["params"];
      string symbol = "";
      if(CheckPointer(qparams2)!=POINTER_INVALID) {
         CJAVal *sym2 = (*qparams2)["symbol"];
         if(CheckPointer(sym2)!=POINTER_INVALID) symbol = sym2.ToStr();
      }
      MqlTick tick;
      if(SymbolInfoTick(symbol,tick)) {
         CJAVal t;
         t["symbol"] = symbol;
         t["bid"] = tick.bid;
         t["ask"] = tick.ask;
         t["last"] = tick.last;
         CJAVal *pvol = t["volume"];
         if(CheckPointer(pvol)!=POINTER_INVALID) (*pvol) = (long)tick.volume;
         t["time"] = (long)tick.time;
         resp["data"] = t;
      } else {
         resp["ok"] = false;
         resp["error"] = "SymbolInfoTick failed";
      }
   } else
   if(action == "get_history_rates") {
      CJAVal *qp = query["params"];
      string symbol = "";
      int timeframe = 0;
      long fromTs = 0;
      long toTs = 0;
      int count = 0;
      if(CheckPointer(qp)!=POINTER_INVALID) {
         CJAVal *vs = (*qp)["symbol"];
         CJAVal *vtf = (*qp)["timeframe"];
         CJAVal *vfrom = (*qp)["from"];
         CJAVal *vto = (*qp)["to"];
         CJAVal *vc = (*qp)["count"];
         if(CheckPointer(vs)!=POINTER_INVALID) symbol = vs.ToStr();
         if(CheckPointer(vtf)!=POINTER_INVALID) timeframe = (int)vtf.ToInt();
         if(CheckPointer(vfrom)!=POINTER_INVALID) fromTs = vfrom.ToInt();
         if(CheckPointer(vto)!=POINTER_INVALID) toTs = vto.ToInt();
         if(CheckPointer(vc)!=POINTER_INVALID) count = (int)vc.ToInt();
      }
      
      MqlRates rates[];
      int copied = 0;
      
      // Prefer explicit range when both from/to provided
      if(fromTs>0 && toTs>0) {
         datetime from = (datetime)fromTs;
         datetime to = (datetime)toTs;
         copied = CopyRates(symbol,(ENUM_TIMEFRAMES)timeframe,from,to,rates);
      } else
      if(fromTs>0 && count>0) {
         datetime from = (datetime)fromTs;
         copied = CopyRates(symbol,(ENUM_TIMEFRAMES)timeframe,from,count,rates);
      } else {
         // fallback: latest count (default 100)
         int c = (count>0?count:100);
         copied = CopyRates(symbol,(ENUM_TIMEFRAMES)timeframe,0,c,rates);
      }
      
      if(copied<=0) {
         resp["ok"] = false;
         resp["error"] = "CopyRates returned 0";
      } else {
         CJAVal arr;
         for(int i=0;i<ArraySize(rates);i++) {
            CJAVal r;
            r["time"] = (long)rates[i].time;
            r["open"] = rates[i].open;
            r["high"] = rates[i].high;
            r["low"] = rates[i].low;
            r["close"] = rates[i].close;
            CJAVal *ptv = r["tick_volume"];
            if(CheckPointer(ptv)!=POINTER_INVALID) (*ptv) = (long)rates[i].tick_volume;
            r["spread"] = (int)rates[i].spread;
            CJAVal *prv = r["real_volume"];
            if(CheckPointer(prv)!=POINTER_INVALID) (*prv) = (long)rates[i].real_volume;
            arr.Add(r);
         }
         resp["data"] = arr;
      }
   } else
   if(action == "get_history_ticks") {
      CJAVal *qp2 = query["params"];
      string symbol = "";
      long fromTs = 0;
      long toTs = 0;
      int flags = 0;
      int count = 0;
      if(CheckPointer(qp2)!=POINTER_INVALID) {
         CJAVal *vs2 = (*qp2)["symbol"];
         CJAVal *vfrom2 = (*qp2)["from"];
         CJAVal *vto2 = (*qp2)["to"];
         CJAVal *vflags = (*qp2)["flags"];
         CJAVal *vcount = (*qp2)["count"];
         if(CheckPointer(vs2)!=POINTER_INVALID) symbol = vs2.ToStr();
         if(CheckPointer(vfrom2)!=POINTER_INVALID) fromTs = vfrom2.ToInt();
         if(CheckPointer(vto2)!=POINTER_INVALID) toTs = vto2.ToInt();
         if(CheckPointer(vflags)!=POINTER_INVALID) flags = (int)vflags.ToInt();
         if(CheckPointer(vcount)!=POINTER_INVALID) count = (int)vcount.ToInt();
      }
      
      MqlTick ticks[];
      int copied = 0;
      if(fromTs>0 && toTs>0) {
          datetime from = (datetime)fromTs;
          datetime to = (datetime)toTs;
          // CopyTicksRange signature: (symbol, ticks[], from, to, flags)
          copied = CopyTicksRange(symbol,ticks,from,to,flags);
      } else
      if(fromTs>0 && count>0) {
         datetime from = (datetime)fromTs;
         // CopyTicks signature: CopyTicks(symbol, ticks[], flags, from, count)
         copied = CopyTicks(symbol, ticks, flags, from, (uint)count);
      } else {
         // default latest 1000 ticks
         datetime from = (datetime)(TimeCurrent() - 3600);
         copied = CopyTicks(symbol, ticks, flags, from, (uint)(count>0?count:1000));
      }
      
      if(copied<=0) {
         resp["ok"] = false;
         resp["error"] = "CopyTicks* returned 0";
      } else {
         CJAVal arr;
         for(int i=0;i<ArraySize(ticks);i++) {
            CJAVal t;
            t["time"] = (long)ticks[i].time;
            t["bid"] = ticks[i].bid;
            t["ask"] = ticks[i].ask;
            t["last"] = ticks[i].last;
            CJAVal *pv = t["volume"];
            if(CheckPointer(pv)!=POINTER_INVALID) (*pv) = (long)ticks[i].volume;
            t["flags"] = (int)ticks[i].flags;
            arr.Add(t);
         }
         resp["data"] = arr;
      }
   } else
   if(action == "get_balance_history") {
      CJAVal *qp3 = query["params"];
      long fromTs = 0;
      long toTs = 0;
      if(CheckPointer(qp3)!=POINTER_INVALID) {
         CJAVal *vf = (*qp3)["from"];
         CJAVal *vt = (*qp3)["to"];
         if(CheckPointer(vf)!=POINTER_INVALID) fromTs = vf.ToInt();
         if(CheckPointer(vt)!=POINTER_INVALID) toTs = vt.ToInt();
      }
      datetime from = (fromTs>0 ? (datetime)fromTs : (datetime)(TimeCurrent() - 30*24*60*60)); // default 30d
      datetime to = (toTs>0 ? (datetime)toTs : TimeCurrent());
      
      if(!HistorySelect(from, to)) {
         resp["ok"] = false;
         resp["error"] = "HistorySelect failed";
      } else {
         int total = HistoryDealsTotal();
         CJAVal events;
         for(int i=0;i<total;i++) {
            ulong dealTicket = HistoryDealGetTicket(i);
            if(dealTicket<=0) continue;
            
            int dtype = (int)HistoryDealGetInteger(dealTicket, DEAL_TYPE);
            // Balance-affecting non-trade operations + commissions/fees
            bool include = (dtype == DEAL_TYPE_BALANCE || dtype == DEAL_TYPE_CREDIT || dtype == DEAL_TYPE_CHARGE
                            || dtype == DEAL_TYPE_BONUS || dtype == DEAL_TYPE_COMMISSION || dtype == DEAL_TYPE_COMMISSION_DAILY
                            || dtype == DEAL_TYPE_COMMISSION_MONTHLY || dtype == DEAL_TYPE_INTEREST);
            if(!include) continue;
            
            CJAVal e;
            e["ticket"] = (long)dealTicket;
            e["time"] = (long)HistoryDealGetInteger(dealTicket, DEAL_TIME);
            e["type"] = dtype;
            e["comment"] = HistoryDealGetString(dealTicket, DEAL_COMMENT);
            e["profit"] = HistoryDealGetDouble(dealTicket, DEAL_PROFIT); // positive deposit/credit, negative withdrawals/fees
            events.Add(e);
         }
         resp["data"] = events;
      }
   } else
   if(action == "get_equity_history") {
      CJAVal *qp4 = query["params"];
      long fromTs = 0;
      long toTs = 0;
      if(CheckPointer(qp4)!=POINTER_INVALID) {
         CJAVal *vf4 = (*qp4)["from"];
         CJAVal *vt4 = (*qp4)["to"];
         if(CheckPointer(vf4)!=POINTER_INVALID) fromTs = vf4.ToInt();
         if(CheckPointer(vt4)!=POINTER_INVALID) toTs = vt4.ToInt();
      }
      datetime from = (fromTs>0 ? (datetime)fromTs : (datetime)(TimeCurrent() - 30*24*60*60));
      datetime to = (toTs>0 ? (datetime)toTs : TimeCurrent());
      
      if(!HistorySelect(from, to)) {
         resp["ok"] = false;
         resp["error"] = "HistorySelect failed";
      } else {
         // Build cumulative closed P&L over time (equity_closed curve)
         int total = HistoryDealsTotal();
         
         // Collect trade deals only (buy/sell) and accumulate profit
         struct Pnt { long t; double v; };
         Pnt points[];
         
         for(int i=0;i<total;i++) {
            ulong dealTicket = HistoryDealGetTicket(i);
            if(dealTicket<=0) continue;
            int dtype = (int)HistoryDealGetInteger(dealTicket, DEAL_TYPE);
            if(dtype != DEAL_TYPE_BUY && dtype != DEAL_TYPE_SELL) continue;
            long dtm = (long)HistoryDealGetInteger(dealTicket, DEAL_TIME);
            double profit = HistoryDealGetDouble(dealTicket, DEAL_PROFIT);
            int sz = ArraySize(points);
            ArrayResize(points, sz+1);
            points[sz].t = dtm;
            points[sz].v = profit;
         }
         
         // Sort by time (insertion sort for small arrays)
         int n = ArraySize(points);
         for(int i=1;i<n;i++) {
            Pnt key = points[i];
            int j = i-1;
            while(j>=0 && points[j].t > key.t) {
               points[j+1] = points[j];
               j--;
            }
            points[j+1] = key;
         }
         
         CJAVal arr;
         double cumulative = 0.0;
         for(int i=0;i<n;i++) {
            cumulative += points[i].v;
            CJAVal e;
            e["time"] = (long)points[i].t;
            e["equity_closed"] = cumulative;
            arr.Add(e);
         }
         resp["data"] = arr;
      }
   } else
   if(action == "get_statement") {
      CJAVal *qp5 = query["params"];
      long fromTs = 0;
      long toTs = 0;
      if(CheckPointer(qp5)!=POINTER_INVALID) {
         CJAVal *vf5 = (*qp5)["from"];
         CJAVal *vt5 = (*qp5)["to"];
         if(CheckPointer(vf5)!=POINTER_INVALID) fromTs = vf5.ToInt();
         if(CheckPointer(vt5)!=POINTER_INVALID) toTs = vt5.ToInt();
      }
      datetime from = (fromTs>0 ? (datetime)fromTs : (datetime)(TimeCurrent() - 30*24*60*60));
      datetime to = (toTs>0 ? (datetime)toTs : TimeCurrent());
      
      CJAVal st;
      CJAVal st_acc;
      st_acc["login"] = (string)AccountInfoInteger(ACCOUNT_LOGIN);
      st_acc["currency"] = AccountInfoString(ACCOUNT_CURRENCY);
      st_acc["leverage"] = (int)AccountInfoInteger(ACCOUNT_LEVERAGE);
      st["account"] = st_acc;
      CJAVal st_period;
      st_period["from"] = (long)from;
      st_period["to"] = (long)to;
      st["period"] = st_period;
      
      if(!HistorySelect(from,to)) {
         resp["ok"] = false;
         resp["error"] = "HistorySelect failed";
      } else {
         // Orders history
         CJAVal orders;
         int ototal = HistoryOrdersTotal();
         for(int i=0;i<ototal;i++) {
            ulong ot = HistoryOrderGetTicket(i);
            if(ot<=0) continue;
            CJAVal o;
            o["ticket"] = (long)ot;
            o["symbol"] = HistoryOrderGetString(ot, ORDER_SYMBOL);
            o["type"] = (int)HistoryOrderGetInteger(ot, ORDER_TYPE);
            o["time_setup"] = (long)HistoryOrderGetInteger(ot, ORDER_TIME_SETUP);
            o["price_open"] = HistoryOrderGetDouble(ot, ORDER_PRICE_OPEN);
            o["volume_initial"] = HistoryOrderGetDouble(ot, ORDER_VOLUME_INITIAL);
            o["volume_current"] = HistoryOrderGetDouble(ot, ORDER_VOLUME_CURRENT);
            orders.Add(o);
         }
         st["orders"] = orders;
         
         // Deals history
         CJAVal deals;
         int dtotal = HistoryDealsTotal();
         double totalProfit = 0.0;
         for(int i=0;i<dtotal;i++) {
            ulong dticket = HistoryDealGetTicket(i);
            if(dticket<=0) continue;
            CJAVal d;
            d["ticket"] = (long)dticket;
            d["time"] = (long)HistoryDealGetInteger(dticket, DEAL_TIME);
            d["type"] = (int)HistoryDealGetInteger(dticket, DEAL_TYPE);
            d["entry"] = (int)HistoryDealGetInteger(dticket, DEAL_ENTRY);
            d["symbol"] = HistoryDealGetString(dticket, DEAL_SYMBOL);
            d["price"] = HistoryDealGetDouble(dticket, DEAL_PRICE);
            d["volume"] = HistoryDealGetDouble(dticket, DEAL_VOLUME);
            d["profit"] = HistoryDealGetDouble(dticket, DEAL_PROFIT);
            d["comment"] = HistoryDealGetString(dticket, DEAL_COMMENT);
            deals.Add(d);
            CJAVal *pprofit = d["profit"];
            if(CheckPointer(pprofit)!=POINTER_INVALID) totalProfit += pprofit.ToDbl();
         }
         st["deals"] = deals;
         CJAVal st_summary;
         st_summary["closed_profit"] = totalProfit;
         st["summary"] = st_summary;
      }
      
      resp["data"] = st;
   } else {
      handled = false;
      resp["ok"] = false;
      resp["error"] = "Unknown action";
   }
   
   if(handled) {
      Print("✅ Query handled | action=", action, " | requestId=", requestId);
      SendData(resp.Serialize());
   } else {
      Print("⚠️ Unknown query action | action=", action, " | requestId=", requestId);
      SendData(resp.Serialize());
   }
}
