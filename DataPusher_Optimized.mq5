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

// Forward declarations
void SendAccountSnapshot();
void SendQuickUpdate();
void SendPositionsUpdate();
void SendOrdersUpdate();
string GetCurrentTimestamp();
void ProcessSocketError(string functionName, int errorCode);
void SendData(string data);
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

