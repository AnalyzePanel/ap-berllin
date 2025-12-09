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

// Version information
#define EXPERT_VERSION "1.0.0"
#define EXPERT_BUILD_DATE __DATE__

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

// Rules variables
double maxDailyDrawdownPercent = 0.0;    // MAX_DAILY_DRAWDOWN_PERCENT
double maxTotalDrawdownPercent = 0.0;    // MAX_TOTAL_DRAWDOWN_PERCENT
double floatingRiskPercent = 0.0;        // FLOATING_RISK_PERCENT
double marathonStartBalance = 0.0;       // Marathon start balance for total drawdown calculation
datetime lastRuleCheckTime = 0;          // Last time rules were checked
string failedRules = "";                 // Comma-separated list of failed rule types
string failedReasons = "";               // Comma-separated list of failure reasons
bool rulesInitialized = false;           // Whether rules have been set
bool rulesWerePassing = true;            // Track if rules were passing on last check (for violation alerts)

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
void UpdateDailyStartBalance();
string GetDailyStartBalanceVarName(string login, int date);
double GetDailyStartBalance(string login, int date);
void SetDailyStartBalance(string login, int date, double balance);
void SetRules(CJAVal &rulesArray);
void GetRulesStatus(CJAVal &status);
void CheckRulesStatus();
string GetRuleFailureReason(string ruleType, double currentValue, double limit);
double GetCurrentDailyDrawdownPercent();
double GetCurrentTotalDrawdownPercent();
double GetCurrentFloatingRiskPercent();
void SendRuleViolationAlert(string alertFailedRules, string alertFailedReasons);

//+------------------------------------------------------------------+
//| Expert initialization function                                     |
//+------------------------------------------------------------------+
int OnInit() {
   // Log version information first
   Print("========================================");
   Print("DataPusher Optimized Expert Advisor");
   Print("Version: ", EXPERT_VERSION);
   Print("Build Date: ", EXPERT_BUILD_DATE);
   Print("========================================");
   
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
   
   // Initialize daily start balance (using terminal global variables)
   UpdateDailyStartBalance();
   
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
   
   // -1. Check and update daily start balance if day changed
   UpdateDailyStartBalance();

   // -2. Check rules status on every tick (if rules are initialized)
   if(rulesInitialized) {
      CheckRulesStatus();
   }

   // -3. Poll for incoming server queries (non-blocking)
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
   
   // SendData(data.Serialize());
   
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
   data["balance"] = AccountInfoDouble(ACCOUNT_BALANCE);
   data["equity"] = AccountInfoDouble(ACCOUNT_EQUITY);
   data["profit"] = AccountInfoDouble(ACCOUNT_PROFIT);
   
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
//| Get global variable name for daily start balance                  |
//+------------------------------------------------------------------+
string GetDailyStartBalanceVarName(string login, int date) {
   return "DailyStartBalance_" + login + "_" + IntegerToString(date);
}

//+------------------------------------------------------------------+
//| Get daily start balance from terminal global variable             |
//+------------------------------------------------------------------+
double GetDailyStartBalance(string login, int date) {
   string varName = GetDailyStartBalanceVarName(login, date);
   if(GlobalVariableCheck(varName)) {
      return GlobalVariableGet(varName);
   }
   return 0.0;
}

//+------------------------------------------------------------------+
//| Set daily start balance to terminal global variable               |
//+------------------------------------------------------------------+
void SetDailyStartBalance(string login, int date, double balance) {
   string varName = GetDailyStartBalanceVarName(login, date);
   GlobalVariableSet(varName, balance);
   GlobalVariablesFlush(); // Force save to disk
}

//+------------------------------------------------------------------+
//| Update daily start balance if day changed or not initialized      |
//+------------------------------------------------------------------+
void UpdateDailyStartBalance() {
   string currentLogin = (string)AccountInfoInteger(ACCOUNT_LOGIN);
   MqlDateTime dt;
   TimeCurrent(dt);
   int currentDate = dt.year * 10000 + dt.mon * 100 + dt.day;
   
   string varName = GetDailyStartBalanceVarName(currentLogin, currentDate);
   double currentBalance = AccountInfoDouble(ACCOUNT_BALANCE);
   
   // Check if global variable exists for today
   // If it doesn't exist, it means it's a new day or first run - save current balance
   if(!GlobalVariableCheck(varName)) {
      SetDailyStartBalance(currentLogin, currentDate, currentBalance);
      Print("📅 Daily start balance updated: login=", currentLogin, " | date=", currentDate, " | balance=", currentBalance);
   }
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
   string requestId = "";
   if(CheckPointer(reqIdPtr) != POINTER_INVALID && reqIdPtr != NULL) {
      requestId = reqIdPtr.ToStr();
   } else {
      // Defensive: keep empty but log to ease tracing when requestId is missing
      Print("⚠️ Query received without valid requestId pointer, echoing empty requestId");
   }
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
   if(action == "get_daily_start_balance") {
      // Return today's start balance from terminal global variable
      UpdateDailyStartBalance(); // Ensure it's up to date
      string currentLogin = (string)AccountInfoInteger(ACCOUNT_LOGIN);
      MqlDateTime dt;
      TimeCurrent(dt);
      int currentDate = dt.year * 10000 + dt.mon * 100 + dt.day;
      double balance = GetDailyStartBalance(currentLogin, currentDate);
      
      CJAVal data;
      data["login"] = currentLogin;
      data["date"] = currentDate;
      data["balance"] = balance;
      resp["data"] = data;
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
      
      // 1) Compute base balance at 'from' by backing out deltas from (from, now]
      datetime nowdt = TimeCurrent();
      double baseBalance = AccountInfoDouble(ACCOUNT_BALANCE);
      if(HistorySelect(from, nowdt)) {
         int totb = HistoryDealsTotal();
         for(int i=0;i<totb;i++) {
            ulong deal = HistoryDealGetTicket(i);
            if(deal<=0) continue;
            long ts = (long)HistoryDealGetInteger(deal, DEAL_TIME);
            if(ts <= from) continue;
            int dtype = (int)HistoryDealGetInteger(deal, DEAL_TYPE);
            int entry = (int)HistoryDealGetInteger(deal, DEAL_ENTRY);
            double dprof = HistoryDealGetDouble(deal, DEAL_PROFIT);
            bool isBalanceAffect = (dtype == DEAL_TYPE_BALANCE || dtype == DEAL_TYPE_CREDIT || dtype == DEAL_TYPE_CHARGE
                                    || dtype == DEAL_TYPE_BONUS || dtype == DEAL_TYPE_COMMISSION || dtype == DEAL_TYPE_COMMISSION_DAILY
                                    || dtype == DEAL_TYPE_COMMISSION_MONTHLY || dtype == DEAL_TYPE_INTEREST
                                    || dtype == DEAL_TYPE_BUY || dtype == DEAL_TYPE_SELL);
            // For trades, only count realized (exit) profit into balance
            if((dtype == DEAL_TYPE_BUY || dtype == DEAL_TYPE_SELL) && entry != DEAL_ENTRY_OUT) isBalanceAffect = false;
            if(!isBalanceAffect) continue;
            baseBalance -= dprof;
         }
      }
      
      // 2) Build in-range events and cumulative absolute balance after each event
      if(!HistorySelect(from, to)) {
         resp["ok"] = false;
         resp["error"] = "HistorySelect failed";
      } else {
         // Collect and sort by time asc (History* lists are typically time-sorted, but ensure)
         struct BEvent { long t; double delta; ulong ticket; int dtype; string comment; };
         BEvent evs[];
         int total = HistoryDealsTotal();
         for(int i=0;i<total;i++) {
            ulong dealTicket = HistoryDealGetTicket(i);
            if(dealTicket<=0) continue;
            int dtype = (int)HistoryDealGetInteger(dealTicket, DEAL_TYPE);
            int entry = (int)HistoryDealGetInteger(dealTicket, DEAL_ENTRY);
            // Include balance-affecting ops and realized trade profit (exit deals)
            bool include = (dtype == DEAL_TYPE_BALANCE || dtype == DEAL_TYPE_CREDIT || dtype == DEAL_TYPE_CHARGE
                            || dtype == DEAL_TYPE_BONUS || dtype == DEAL_TYPE_COMMISSION || dtype == DEAL_TYPE_COMMISSION_DAILY
                            || dtype == DEAL_TYPE_COMMISSION_MONTHLY || dtype == DEAL_TYPE_INTEREST
                            || ((dtype == DEAL_TYPE_BUY || dtype == DEAL_TYPE_SELL) && entry == DEAL_ENTRY_OUT));
            if(!include) continue;
            long tme = (long)HistoryDealGetInteger(dealTicket, DEAL_TIME);
            double dlt = HistoryDealGetDouble(dealTicket, DEAL_PROFIT);
            int szb = ArraySize(evs);
            ArrayResize(evs, szb+1);
            evs[szb].t = tme;
            evs[szb].delta = dlt;
            evs[szb].ticket = dealTicket;
            evs[szb].dtype = dtype;
            evs[szb].comment = HistoryDealGetString(dealTicket, DEAL_COMMENT);
         }
         // Insertion sort by time
         int nb = ArraySize(evs);
         for(int i=1;i<nb;i++) {
            BEvent key = evs[i];
            int j = i-1;
            while(j>=0 && evs[j].t > key.t) { evs[j+1]=evs[j]; j--; }
            evs[j+1] = key;
         }
         // Build response with running absolute balance
         CJAVal events;
         double running = baseBalance;
         for(int i=0;i<nb;i++) {
            running += evs[i].delta;
            CJAVal e;
            e["time"] = (long)evs[i].t;
            e["ticket"] = (long)evs[i].ticket;
            e["type"] = evs[i].dtype;
            e["delta"] = evs[i].delta;
            e["balance"] = running; // absolute balance after this event
            e["comment"] = evs[i].comment;
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
      
      // Equity curve should be based on balance plus all realized changes, same base as balance history.
      // So compute baseAtFrom exactly like for balance history (balance at 'from').
      datetime nowdt2 = TimeCurrent();
      double baseAtFrom = AccountInfoDouble(ACCOUNT_BALANCE);
      if(HistorySelect(from, nowdt2)) {
         int tot2 = HistoryDealsTotal();
         for(int i=0;i<tot2;i++) {
            ulong deal2 = HistoryDealGetTicket(i);
            if(deal2<=0) continue;
            long ts2 = (long)HistoryDealGetInteger(deal2, DEAL_TIME);
            if(ts2 <= from) continue;
            int dtype2 = (int)HistoryDealGetInteger(deal2, DEAL_TYPE);
            int entry2 = (int)HistoryDealGetInteger(deal2, DEAL_ENTRY);
            double pr2 = HistoryDealGetDouble(deal2, DEAL_PROFIT);
            bool includeBal = (dtype2 == DEAL_TYPE_BALANCE || dtype2 == DEAL_TYPE_CREDIT || dtype2 == DEAL_TYPE_CHARGE
                               || dtype2 == DEAL_TYPE_BONUS || dtype2 == DEAL_TYPE_COMMISSION || dtype2 == DEAL_TYPE_COMMISSION_DAILY
                               || dtype2 == DEAL_TYPE_COMMISSION_MONTHLY || dtype2 == DEAL_TYPE_INTEREST
                               || ((dtype2 == DEAL_TYPE_BUY || dtype2 == DEAL_TYPE_SELL) && entry2 == DEAL_ENTRY_OUT));
            if(!includeBal) continue;
            baseAtFrom -= pr2;
         }
      }

      if(!HistorySelect(from, to)) {
         resp["ok"] = false;
         resp["error"] = "HistorySelect failed";
      } else {
         // Build equity curve using same deltas as balance (deposits, withdrawals, commissions, realized trades).
         struct EqEv { long t; double delta; };
         EqEv eqevs[];
         int total = HistoryDealsTotal();
         for(int i=0;i<total;i++) {
            ulong dticket = HistoryDealGetTicket(i);
            if(dticket<=0) continue;
            int dtype = (int)HistoryDealGetInteger(dticket, DEAL_TYPE);
            int entry = (int)HistoryDealGetInteger(dticket, DEAL_ENTRY);
            bool includeEq = (dtype == DEAL_TYPE_BALANCE || dtype == DEAL_TYPE_CREDIT || dtype == DEAL_TYPE_CHARGE
                              || dtype == DEAL_TYPE_BONUS || dtype == DEAL_TYPE_COMMISSION || dtype == DEAL_TYPE_COMMISSION_DAILY
                              || dtype == DEAL_TYPE_COMMISSION_MONTHLY || dtype == DEAL_TYPE_INTEREST
                              || ((dtype == DEAL_TYPE_BUY || dtype == DEAL_TYPE_SELL) && entry == DEAL_ENTRY_OUT));
            if(!includeEq) continue;
            long tme = (long)HistoryDealGetInteger(dticket, DEAL_TIME);
            double dlt = HistoryDealGetDouble(dticket, DEAL_PROFIT);
            int sz = ArraySize(eqevs);
            ArrayResize(eqevs, sz+1);
            eqevs[sz].t = tme;
            eqevs[sz].delta = dlt;
         }
         // Sort by time asc
         int n = ArraySize(eqevs);
         for(int i=1;i<n;i++) {
            EqEv key = eqevs[i];
            int j = i-1;
            while(j>=0 && eqevs[j].t > key.t) { eqevs[j+1]=eqevs[j]; j--; }
            eqevs[j+1] = key;
         }
         CJAVal arr;
         double eq = baseAtFrom;
         for(int i=0;i<n;i++) {
            eq += eqevs[i].delta;
            CJAVal e;
            e["time"] = (long)eqevs[i].t;
            e["equity"] = eq; // equity based on balance + all realized changes
            arr.Add(e);
         }
         resp["data"] = arr;
      }
   } else
   if(action == "get_performance_report") {
      CJAVal *qp6 = query["params"];
      long fromTs = 0;
      long toTs = 0;
      if(CheckPointer(qp6)!=POINTER_INVALID) {
         CJAVal *vf6 = (*qp6)["from"];
         CJAVal *vt6 = (*qp6)["to"];
         if(CheckPointer(vf6)!=POINTER_INVALID) fromTs = vf6.ToInt();
         if(CheckPointer(vt6)!=POINTER_INVALID) toTs = vt6.ToInt();
      }
      datetime from = (fromTs>0 ? (datetime)fromTs : (datetime)(TimeCurrent() - 30*24*60*60));
      datetime to = (toTs>0 ? (datetime)toTs : TimeCurrent());

      if(!HistorySelect(from,to)) {
         resp["ok"] = false;
         resp["error"] = "HistorySelect failed";
      } else {
         // ---- Account snapshot-style fields (current) ----
         CJAVal rep;
         rep["balance"] = AccountInfoDouble(ACCOUNT_BALANCE);
         rep["equity"] = AccountInfoDouble(ACCOUNT_EQUITY);
         rep["profit"] = AccountInfoDouble(ACCOUNT_PROFIT);
         rep["margin"] = AccountInfoDouble(ACCOUNT_MARGIN);
         rep["free_margin"] = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
         rep["margin_level"] = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
         rep["credit_facility"] = AccountInfoDouble(ACCOUNT_CREDIT);

         // ---- Trade statistics over selected history ----
         int dtotal = HistoryDealsTotal();
         int totalTrades = 0;
         int profitTrades = 0;
         int lossTrades = 0;
         double grossProfit = 0.0;
         double grossLoss = 0.0;
         double netProfit = 0.0;
         double largestProfit = 0.0;
         double largestLoss = 0.0;

         // For Sharpe-like ratio: store per-trade profits
         double profits[];

         // For consecutive wins/losses
         int curWinStreak = 0, maxWinStreak = 0;
         int curLossStreak = 0, maxLossStreak = 0;
         double curWinSum = 0.0, maxWinSum = 0.0;
         double curLossSum = 0.0, maxLossSum = 0.0;

         // Drawdown using closed-equity curve
         // First reconstruct balance at 'from'
         datetime nowdt3 = TimeCurrent();
         double baseBalFrom = AccountInfoDouble(ACCOUNT_BALANCE);
         if(HistorySelect(from, nowdt3)) {
            int totb2 = HistoryDealsTotal();
            for(int i=0;i<totb2;i++) {
               ulong dealb = HistoryDealGetTicket(i);
               if(dealb<=0) continue;
               long tsb = (long)HistoryDealGetInteger(dealb, DEAL_TIME);
               if(tsb <= from) continue;
               int dtb = (int)HistoryDealGetInteger(dealb, DEAL_TYPE);
               int enb = (int)HistoryDealGetInteger(dealb, DEAL_ENTRY);
               double prb = HistoryDealGetDouble(dealb, DEAL_PROFIT);
               bool affect = (dtb == DEAL_TYPE_BALANCE || dtb == DEAL_TYPE_CREDIT || dtb == DEAL_TYPE_CHARGE
                              || dtb == DEAL_TYPE_BONUS || dtb == DEAL_TYPE_COMMISSION || dtb == DEAL_TYPE_COMMISSION_DAILY
                              || dtb == DEAL_TYPE_COMMISSION_MONTHLY || dtb == DEAL_TYPE_INTEREST
                              || ((dtb == DEAL_TYPE_BUY || dtb == DEAL_TYPE_SELL) && enb == DEAL_ENTRY_OUT));
               if(!affect) continue;
               baseBalFrom -= prb;
            }
         }
         
         // Re-select [from,to] for stats processing
         HistorySelect(from,to);

         struct TDeal { long t; double p; };
         TDeal tdeals[];
         for(int i=0;i<dtotal;i++) {
            ulong dticket = HistoryDealGetTicket(i);
            if(dticket<=0) continue;
            int dtype = (int)HistoryDealGetInteger(dticket, DEAL_TYPE);
            int entry = (int)HistoryDealGetInteger(dticket, DEAL_ENTRY);
            if(dtype != DEAL_TYPE_BUY && dtype != DEAL_TYPE_SELL) continue;
            if(entry != DEAL_ENTRY_OUT) continue; // only closed portions
            double pr = HistoryDealGetDouble(dticket, DEAL_PROFIT);
            long tt = (long)HistoryDealGetInteger(dticket, DEAL_TIME);

            int sz = ArraySize(tdeals);
            ArrayResize(tdeals, sz+1);
            tdeals[sz].t = tt;
            tdeals[sz].p = pr;
         }
         // sort trades by time
         int nt = ArraySize(tdeals);
         for(int i=1;i<nt;i++) {
            TDeal key = tdeals[i];
            int j=i-1;
            while(j>=0 && tdeals[j].t > key.t) { tdeals[j+1]=tdeals[j]; j--; }
            tdeals[j+1] = key;
         }

         // Iterate trades to build stats and drawdown curve
         double eqCurve = baseBalFrom;
         double peak = eqCurve;
         double maxDdAbs = 0.0;

         // Daily and total drawdown tracking
         // Use base balance at 'from' as initial balance
         double initialBalance = baseBalFrom;
         double dailyStartBalance = 0.0;
         double dailyPeak = 0.0;
         double dailyMin = 0.0;
         double overallMin = baseBalFrom;
         double maxDailyDrawdown = 0.0;
         double maxTotalDrawdown = 0.0;
         double localMaxDailyDrawdownPercent = 0.0;
         double localMaxTotalDrawdownPercent = 0.0;
         int currentDay = -1;
         double lastDailyStartBalance = 0.0;
         bool dailyInitialized = false;

         ArrayResize(profits, nt);
         for(int i=0;i<nt;i++) {
            double pr = tdeals[i].p;
            profits[i] = pr;
            totalTrades++;
            netProfit += pr;
            if(pr > 0.0) {
               profitTrades++;
               grossProfit += pr;
               if(pr > largestProfit || profitTrades==1) largestProfit = pr;
               // win streak
               curWinStreak++;
               curWinSum += pr;
               // reset loss streak
               if(curWinStreak>maxWinStreak) {
                  maxWinStreak = curWinStreak;
                  if(curWinSum>maxWinSum) maxWinSum = curWinSum;
               }
               curLossStreak = 0;
               curLossSum = 0.0;
            } else if(pr < 0.0) {
               lossTrades++;
               grossLoss += pr; // negative
               if(pr < largestLoss || lossTrades==1) largestLoss = pr;
               // loss streak
               curLossStreak++;
               curLossSum += pr;
               if(curLossStreak>maxLossStreak) {
                  maxLossStreak = curLossStreak;
                  if(curLossSum<maxLossSum) maxLossSum = curLossSum;
               }
               curWinStreak = 0;
               curWinSum = 0.0;
            }

            // Check for day change BEFORE applying profit
            MqlDateTime tradeDate;
            TimeToStruct((datetime)tdeals[i].t, tradeDate);
            int tradeDay = tradeDate.year * 10000 + tradeDate.mon * 100 + tradeDate.day;
            
            // Check for day change and set daily start balance BEFORE applying profit
            if(currentDay == -1) {
               // First trade - initialize daily tracking with balance BEFORE this trade
               currentDay = tradeDay;
               dailyStartBalance = eqCurve; // Balance before first trade (start of day)
               dailyPeak = eqCurve;
               dailyMin = eqCurve;
               lastDailyStartBalance = dailyStartBalance;
               dailyInitialized = true;
            } else if(tradeDay != currentDay) {
               // Day changed - reset daily tracking for new day
               // Use balance BEFORE first trade of new day as start of new day
               currentDay = tradeDay;
               dailyStartBalance = eqCurve; // Balance at start of new day (before first trade of new day)
               dailyPeak = eqCurve;
               dailyMin = eqCurve;
               lastDailyStartBalance = dailyStartBalance;
            }
            
            // Apply profit to equity curve
            eqCurve += pr;
            
            // Update daily peak and minimum
            if(eqCurve > dailyPeak) dailyPeak = eqCurve;
            if(eqCurve < dailyMin) dailyMin = eqCurve;
            
            // Update overall minimum
            if(eqCurve < overallMin) overallMin = eqCurve;
            
            // Calculate daily drawdown (from daily start balance)
            // Daily drawdown = drop from daily start balance
            double dailyDD = dailyStartBalance - eqCurve;
            if(dailyDD > maxDailyDrawdown) {
               maxDailyDrawdown = dailyDD;
               // Calculate percentage for this drawdown
               if(dailyStartBalance > 0.0) {
                  double dailyDDPercent = (dailyDD / dailyStartBalance) * 100.0;
                  if(dailyDDPercent > localMaxDailyDrawdownPercent) {
                     localMaxDailyDrawdownPercent = dailyDDPercent;
                  }
               }
            }
            
            // Calculate total drawdown (from initial balance)
            double totalDD = initialBalance - eqCurve;
            if(totalDD > maxTotalDrawdown) {
               maxTotalDrawdown = totalDD;
               // Calculate percentage for this drawdown
               if(initialBalance > 0.0) {
                  double totalDDPercent = (totalDD / initialBalance) * 100.0;
                  if(totalDDPercent > localMaxTotalDrawdownPercent) {
                     localMaxTotalDrawdownPercent = totalDDPercent;
                  }
               }
            }
            
            // Original drawdown calculation (from overall peak)
            if(eqCurve > peak) peak = eqCurve;
            double dd = peak - eqCurve;
            if(dd > maxDdAbs) maxDdAbs = dd;
         }

         // Calculate final daily drawdown for the last day (from daily start balance)
         if(dailyInitialized) {
            double lastDailyDD = dailyStartBalance - dailyMin;
            if(lastDailyDD > maxDailyDrawdown) {
               maxDailyDrawdown = lastDailyDD;
               // Calculate percentage for this drawdown
               if(dailyStartBalance > 0.0) {
                  double dailyDDPercent = (lastDailyDD / dailyStartBalance) * 100.0;
                  if(dailyDDPercent > localMaxDailyDrawdownPercent) {
                     localMaxDailyDrawdownPercent = dailyDDPercent;
                  }
               }
            }
         }
         
         // Calculate final total drawdown
         double lastTotalDD = initialBalance - overallMin;
         if(lastTotalDD > maxTotalDrawdown) {
            maxTotalDrawdown = lastTotalDD;
            // Calculate percentage for this drawdown
            if(initialBalance > 0.0) {
               double totalDDPercent = (lastTotalDD / initialBalance) * 100.0;
               if(totalDDPercent > localMaxTotalDrawdownPercent) {
                  localMaxTotalDrawdownPercent = totalDDPercent;
               }
            }
         }

         // Balance drawdown metrics
         double ddAbs = maxDdAbs;
         double ddRel = (peak>0.0 ? 100.0*ddAbs/peak : 0.0);
         rep["balance_drawdown_absolute"] = ddAbs;
         rep["balance_drawdown_maximal"] = ddAbs;
         rep["balance_drawdown_relative_percent"] = ddRel;
         
         // Use terminal global variable for daily start balance
         UpdateDailyStartBalance(); // Ensure it's up to date
         string currentLogin = (string)AccountInfoInteger(ACCOUNT_LOGIN);
         MqlDateTime todayDt;
         TimeCurrent(todayDt);
         int todayDate = todayDt.year * 10000 + todayDt.mon * 100 + todayDt.day;
         double todayStartBalance = GetDailyStartBalance(currentLogin, todayDate);
         rep["daily_start_balance"] = todayStartBalance;
         
         // Daily and total drawdown metrics (in percent)
         // Use the maximum percentage calculated during the loop
         rep["daily_drawdown_max"] = localMaxDailyDrawdownPercent;
         rep["total_drawdown_max"] = localMaxTotalDrawdownPercent;

         // Basic profit stats
         rep["total_net_profit"] = netProfit;
         rep["gross_profit"] = grossProfit;
         rep["gross_loss"] = grossLoss;
         rep["total_trades"] = totalTrades;
         rep["profit_trades"] = profitTrades;
         rep["loss_trades"] = lossTrades;
         if(totalTrades>0) {
            rep["expected_payoff"] = netProfit / (double)totalTrades;
         } else {
            rep["expected_payoff"] = 0.0;
         }
         if(grossLoss<0.0) {
            rep["profit_factor"] = (grossProfit / MathAbs(grossLoss));
         } else {
            rep["profit_factor"] = 0.0;
         }
         if(ddAbs>0.0) {
            rep["recovery_factor"] = netProfit / ddAbs;
         } else {
            rep["recovery_factor"] = 0.0;
         }

         // Sharpe ratio approximation: mean(profit) / stddev(profit)
         double sharpe = 0.0;
         if(totalTrades>1) {
            double sum=0.0;
            for(int i=0;i<totalTrades;i++) sum += profits[i];
            double mean = sum / (double)totalTrades;
            double var=0.0;
            for(int i=0;i<totalTrades;i++) {
               double d = profits[i]-mean;
               var += d*d;
            }
            var /= (double)(totalTrades-1);
            double sd = MathSqrt(var);
            if(sd>0.0) sharpe = mean/sd;
         }
         rep["sharpe_ratio"] = sharpe;

         // Largest / average trade stats
         rep["largest_profit_trade"] = largestProfit;
         rep["largest_loss_trade"] = largestLoss;
         if(profitTrades>0) rep["average_profit_trade"] = grossProfit / (double)profitTrades;
         else rep["average_profit_trade"] = 0.0;
         if(lossTrades>0) rep["average_loss_trade"] = grossLoss / (double)lossTrades;
         else rep["average_loss_trade"] = 0.0;

         // Consecutive stats
         rep["max_consecutive_wins"] = maxWinStreak;
         rep["max_consecutive_wins_profit"] = maxWinSum;
         rep["max_consecutive_losses"] = maxLossStreak;
         rep["max_consecutive_losses_loss"] = maxLossSum;

         // Average consecutive wins/losses: simple approximation
         rep["average_consecutive_wins"] = (maxWinStreak>0 ? 1.0 : 0.0);
         rep["average_consecutive_losses"] = (maxLossStreak>0 ? 1.0 : 0.0);

         resp["data"] = rep;
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
   } else
   if(action == "get_trade_history") {
      CJAVal *qp7 = query["params"];
      long fromTs = 0;
      long toTs = 0;
      if(CheckPointer(qp7)!=POINTER_INVALID) {
         CJAVal *vf7 = (*qp7)["from"];
         CJAVal *vt7 = (*qp7)["to"];
         if(CheckPointer(vf7)!=POINTER_INVALID) fromTs = vf7.ToInt();
         if(CheckPointer(vt7)!=POINTER_INVALID) toTs = vt7.ToInt();
      }
      datetime from = (fromTs>0 ? (datetime)fromTs : (datetime)(TimeCurrent() - 30*24*60*60)); // default 30d
      datetime to = (toTs>0 ? (datetime)toTs : TimeCurrent());
      
      if(!HistorySelect(from,to)) {
         resp["ok"] = false;
         resp["error"] = "HistorySelect failed";
      } else {
         // Group deals by position to form complete trades
         // Structure: position_id -> { open_deal, close_deal, commission, swap, profit }
         struct TradeData {
            ulong positionId;
            ulong orderTicket;
            datetime openTime;
            datetime closeTime;
            int type; // DEAL_TYPE_BUY or DEAL_TYPE_SELL
            string symbol;
            double volume;
            double openPrice;
            double closePrice;
            double commission;
            double swap;
            double profit;
         };
         
         TradeData tradesMap[];
         int mapSize = 0;
         
         int dtotal2 = HistoryDealsTotal();
         for(int i=0;i<dtotal2;i++) {
            ulong dticket2 = HistoryDealGetTicket(i);
            if(dticket2<=0) continue;
            
            ulong posId = HistoryDealGetInteger(dticket2, DEAL_POSITION_ID);
            if(posId == 0) continue; // Skip deals without position ID
            
            int dtype2 = (int)HistoryDealGetInteger(dticket2, DEAL_TYPE);
            int entry2 = (int)HistoryDealGetInteger(dticket2, DEAL_ENTRY);
            
            // Find or create trade entry for this position
            int tradeIdx = -1;
            for(int j=0;j<mapSize;j++) {
               if(tradesMap[j].positionId == posId) {
                  tradeIdx = j;
                  break;
               }
            }
            
            if(tradeIdx < 0) {
               // New trade - initialize
               ArrayResize(tradesMap, mapSize+1);
               tradeIdx = mapSize;
               tradesMap[tradeIdx].positionId = posId;
               tradesMap[tradeIdx].orderTicket = HistoryDealGetInteger(dticket2, DEAL_ORDER);
               tradesMap[tradeIdx].symbol = HistoryDealGetString(dticket2, DEAL_SYMBOL);
               tradesMap[tradeIdx].volume = HistoryDealGetDouble(dticket2, DEAL_VOLUME);
               tradesMap[tradeIdx].commission = 0.0;
               tradesMap[tradeIdx].swap = 0.0;
               tradesMap[tradeIdx].profit = 0.0;
               mapSize++;
            }
            
            // Accumulate swap from deal property (swap is stored in DEAL_SWAP, not as separate deal type)
            double dealSwap = HistoryDealGetDouble(dticket2, DEAL_SWAP);
            if(dealSwap != 0.0) {
               tradesMap[tradeIdx].swap += dealSwap;
            }
            
            // Update trade data based on deal type
            if(dtype2 == DEAL_TYPE_BUY || dtype2 == DEAL_TYPE_SELL) {
               if(entry2 == DEAL_ENTRY_IN) {
                  // Opening deal
                  tradesMap[tradeIdx].openTime = (datetime)HistoryDealGetInteger(dticket2, DEAL_TIME);
                  tradesMap[tradeIdx].openPrice = HistoryDealGetDouble(dticket2, DEAL_PRICE);
                  tradesMap[tradeIdx].type = dtype2;
                  // Store order ticket for SL lookup
                  if(tradesMap[tradeIdx].orderTicket == 0) {
                     tradesMap[tradeIdx].orderTicket = HistoryDealGetInteger(dticket2, DEAL_ORDER);
                  }
               } else if(entry2 == DEAL_ENTRY_OUT) {
                  // Closing deal
                  tradesMap[tradeIdx].closeTime = (datetime)HistoryDealGetInteger(dticket2, DEAL_TIME);
                  tradesMap[tradeIdx].closePrice = HistoryDealGetDouble(dticket2, DEAL_PRICE);
                  tradesMap[tradeIdx].profit += HistoryDealGetDouble(dticket2, DEAL_PROFIT);
               }
            } else if(dtype2 == DEAL_TYPE_COMMISSION || dtype2 == DEAL_TYPE_COMMISSION_DAILY || dtype2 == DEAL_TYPE_COMMISSION_MONTHLY) {
               tradesMap[tradeIdx].commission += HistoryDealGetDouble(dticket2, DEAL_PROFIT); // Commission is negative profit
            }
         }
         
         // Build response array - only include closed trades (have closeTime)
         CJAVal trades;
         for(int i=0;i<mapSize;i++) {
            if(tradesMap[i].closeTime == 0) continue; // Skip open positions
            
            // Calculate risk per trade
            double risk = 0.0;
            if(tradesMap[i].orderTicket > 0) {
               // Look up order to get stop loss
               if(HistoryOrderSelect(tradesMap[i].orderTicket)) {
                  double sl = HistoryOrderGetDouble(tradesMap[i].orderTicket, ORDER_SL);
                  if(sl > 0) {
                     string sym = tradesMap[i].symbol;
                     double point = SymbolInfoDouble(sym, SYMBOL_POINT);
                     double tickValue = SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_VALUE);
                     double tickSize = SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_SIZE);
                     
                     if(tickSize > 0 && tickValue > 0) {
                        double priceDiff = MathAbs(tradesMap[i].openPrice - sl);
                        double ticks = priceDiff / tickSize;
                        risk = ticks * tickValue * tradesMap[i].volume;
                     } else {
                        // Fallback: use point value
                        double contractSize = SymbolInfoDouble(sym, SYMBOL_TRADE_CONTRACT_SIZE);
                        if(contractSize > 0) {
                           risk = MathAbs(tradesMap[i].openPrice - sl) * tradesMap[i].volume * contractSize / point;
                        }
                     }
                  }
               }
            }
            
            // If risk is still 0 (no SL), use margin as proxy or calculate worst-case
            if(risk == 0.0) {
               // Use margin requirement as risk estimate
               string sym = tradesMap[i].symbol;
               double margin = SymbolInfoDouble(sym, SYMBOL_MARGIN_INITIAL);
               if(margin > 0) {
                  risk = margin * tradesMap[i].volume;
               } else {
                  // Fallback: estimate risk as 2% of entry value
                  double contractSize = SymbolInfoDouble(sym, SYMBOL_TRADE_CONTRACT_SIZE);
                  if(contractSize > 0) {
                     risk = tradesMap[i].openPrice * tradesMap[i].volume * contractSize * 0.02;
                  }
               }
            }
            
            // Calculate balance at trade open time to compute risk percentage
            // Reconstruct balance by subtracting all balance-affecting events after open time
            datetime openTime = tradesMap[i].openTime;
            datetime nowdt4 = TimeCurrent();
            double balanceAtOpen = AccountInfoDouble(ACCOUNT_BALANCE);
            
            // Subtract all balance-affecting events that happened after trade opened
            if(HistorySelect(openTime, nowdt4)) {
               int tot4 = HistoryDealsTotal();
               for(int k=0;k<tot4;k++) {
                  ulong deal4 = HistoryDealGetTicket(k);
                  if(deal4<=0) continue;
                  long ts4 = (long)HistoryDealGetInteger(deal4, DEAL_TIME);
                  if(ts4 <= openTime) continue; // Only events after open time
                  
                  int dtype4 = (int)HistoryDealGetInteger(deal4, DEAL_TYPE);
                  int entry4 = (int)HistoryDealGetInteger(deal4, DEAL_ENTRY);
                  double pr4 = HistoryDealGetDouble(deal4, DEAL_PROFIT);
                  
                  // Include balance-affecting operations and realized trades
                  bool affectBal = (dtype4 == DEAL_TYPE_BALANCE || dtype4 == DEAL_TYPE_CREDIT || dtype4 == DEAL_TYPE_CHARGE
                                   || dtype4 == DEAL_TYPE_BONUS || dtype4 == DEAL_TYPE_COMMISSION || dtype4 == DEAL_TYPE_COMMISSION_DAILY
                                   || dtype4 == DEAL_TYPE_COMMISSION_MONTHLY || dtype4 == DEAL_TYPE_INTEREST
                                   || ((dtype4 == DEAL_TYPE_BUY || dtype4 == DEAL_TYPE_SELL) && entry4 == DEAL_ENTRY_OUT));
                  if(affectBal) {
                     balanceAtOpen -= pr4; // Subtract to get balance at open time
                  }
               }
            }
            
            // Calculate risk as percentage of balance at open time
            double riskPercent = 0.0;
            if(balanceAtOpen > 0 && risk > 0) {
               riskPercent = (risk / balanceAtOpen) * 100.0;
            }
            
            CJAVal t;
            t["open_time"] = (long)tradesMap[i].openTime;
            t["close_time"] = (long)tradesMap[i].closeTime;
            t["type"] = (tradesMap[i].type == DEAL_TYPE_BUY ? "BUY" : "SELL");
            t["volume"] = tradesMap[i].volume;
            t["symbol"] = tradesMap[i].symbol;
            t["open_price"] = tradesMap[i].openPrice;
            t["close_price"] = tradesMap[i].closePrice;
            t["commission"] = tradesMap[i].commission;
            t["swap"] = tradesMap[i].swap;
            t["profit"] = tradesMap[i].profit;
            t["risk"] = risk; // Absolute risk amount
            t["risk_percent"] = riskPercent; // Risk as percentage of balance at open time
            t["balance_at_open"] = balanceAtOpen; // Balance when trade was opened (for reference)
            trades.Add(t);
         }
         resp["data"] = trades;
      }
   } else
   if(action == "get_symbol_statistics") {
      CJAVal *qp8 = query["params"];
      long fromTs = 0;
      long toTs = 0;
      if(CheckPointer(qp8)!=POINTER_INVALID) {
         CJAVal *vf8 = (*qp8)["from"];
         CJAVal *vt8 = (*qp8)["to"];
         if(CheckPointer(vf8)!=POINTER_INVALID) fromTs = vf8.ToInt();
         if(CheckPointer(vt8)!=POINTER_INVALID) toTs = vt8.ToInt();
      }
      datetime from = (fromTs>0 ? (datetime)fromTs : (datetime)(TimeCurrent() - 30*24*60*60));
      datetime to = (toTs>0 ? (datetime)toTs : TimeCurrent());
      
      if(!HistorySelect(from,to)) {
         resp["ok"] = false;
         resp["error"] = "HistorySelect failed";
      } else {
         // Group trades by symbol and calculate statistics
         struct SymbolStats {
            string symbol;
            int totalTrades;
            int profitTrades;
            int lossTrades;
            double totalProfit;
            double totalLoss;
            double grossProfit;
            double grossLoss;
            double netProfit;
            double totalCommission;
            double totalSwap;
            double totalRisk;
            double largestProfit;
            double largestLoss;
            double averageProfit;
            double averageLoss;
            double winRate; // percentage
            double profitFactor;
            double avgRiskPercent;
         };
         
         SymbolStats statsMap[];
         int statsSize = 0;
         
         // First, build trades map (similar to get_trade_history)
         struct TradeData2 {
            ulong positionId;
            ulong orderTicket;
            datetime openTime;
            datetime closeTime;
            int type;
            string symbol;
            double volume;
            double openPrice;
            double closePrice;
            double commission;
            double swap;
            double profit;
            double risk;
            double riskPercent;
         };
         
         TradeData2 tradesMap2[];
         int mapSize2 = 0;
         
         int dtotal3 = HistoryDealsTotal();
         for(int i=0;i<dtotal3;i++) {
            ulong dticket3 = HistoryDealGetTicket(i);
            if(dticket3<=0) continue;
            
            ulong posId = HistoryDealGetInteger(dticket3, DEAL_POSITION_ID);
            if(posId == 0) continue;
            
            int dtype3 = (int)HistoryDealGetInteger(dticket3, DEAL_TYPE);
            int entry3 = (int)HistoryDealGetInteger(dticket3, DEAL_ENTRY);
            
            int tradeIdx = -1;
            for(int j=0;j<mapSize2;j++) {
               if(tradesMap2[j].positionId == posId) {
                  tradeIdx = j;
                  break;
               }
            }
            
            if(tradeIdx < 0) {
               ArrayResize(tradesMap2, mapSize2+1);
               tradeIdx = mapSize2;
               tradesMap2[tradeIdx].positionId = posId;
               tradesMap2[tradeIdx].orderTicket = HistoryDealGetInteger(dticket3, DEAL_ORDER);
               tradesMap2[tradeIdx].symbol = HistoryDealGetString(dticket3, DEAL_SYMBOL);
               tradesMap2[tradeIdx].volume = HistoryDealGetDouble(dticket3, DEAL_VOLUME);
               tradesMap2[tradeIdx].commission = 0.0;
               tradesMap2[tradeIdx].swap = 0.0;
               tradesMap2[tradeIdx].profit = 0.0;
               tradesMap2[tradeIdx].risk = 0.0;
               tradesMap2[tradeIdx].riskPercent = 0.0;
               mapSize2++;
            }
            
            double dealSwap = HistoryDealGetDouble(dticket3, DEAL_SWAP);
            if(dealSwap != 0.0) {
               tradesMap2[tradeIdx].swap += dealSwap;
            }
            
            if(dtype3 == DEAL_TYPE_BUY || dtype3 == DEAL_TYPE_SELL) {
               if(entry3 == DEAL_ENTRY_IN) {
                  tradesMap2[tradeIdx].openTime = (datetime)HistoryDealGetInteger(dticket3, DEAL_TIME);
                  tradesMap2[tradeIdx].openPrice = HistoryDealGetDouble(dticket3, DEAL_PRICE);
                  tradesMap2[tradeIdx].type = dtype3;
                  if(tradesMap2[tradeIdx].orderTicket == 0) {
                     tradesMap2[tradeIdx].orderTicket = HistoryDealGetInteger(dticket3, DEAL_ORDER);
                  }
               } else if(entry3 == DEAL_ENTRY_OUT) {
                  tradesMap2[tradeIdx].closeTime = (datetime)HistoryDealGetInteger(dticket3, DEAL_TIME);
                  tradesMap2[tradeIdx].closePrice = HistoryDealGetDouble(dticket3, DEAL_PRICE);
                  tradesMap2[tradeIdx].profit += HistoryDealGetDouble(dticket3, DEAL_PROFIT);
               }
            } else if(dtype3 == DEAL_TYPE_COMMISSION || dtype3 == DEAL_TYPE_COMMISSION_DAILY || dtype3 == DEAL_TYPE_COMMISSION_MONTHLY) {
               tradesMap2[tradeIdx].commission += HistoryDealGetDouble(dticket3, DEAL_PROFIT);
            }
         }
         
         // Calculate risk for each trade and group by symbol
         datetime nowdt5 = TimeCurrent();
         for(int i=0;i<mapSize2;i++) {
            if(tradesMap2[i].closeTime == 0) continue;
            
            // Calculate risk (same logic as get_trade_history)
            double risk = 0.0;
            if(tradesMap2[i].orderTicket > 0) {
               if(HistoryOrderSelect(tradesMap2[i].orderTicket)) {
                  double sl = HistoryOrderGetDouble(tradesMap2[i].orderTicket, ORDER_SL);
                  if(sl > 0) {
                     string sym = tradesMap2[i].symbol;
                     double point = SymbolInfoDouble(sym, SYMBOL_POINT);
                     double tickValue = SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_VALUE);
                     double tickSize = SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_SIZE);
                     
                     if(tickSize > 0 && tickValue > 0) {
                        double priceDiff = MathAbs(tradesMap2[i].openPrice - sl);
                        double ticks = priceDiff / tickSize;
                        risk = ticks * tickValue * tradesMap2[i].volume;
                     } else {
                        double contractSize = SymbolInfoDouble(sym, SYMBOL_TRADE_CONTRACT_SIZE);
                        if(contractSize > 0) {
                           risk = MathAbs(tradesMap2[i].openPrice - sl) * tradesMap2[i].volume * contractSize / point;
                        }
                     }
                  }
               }
            }
            
            if(risk == 0.0) {
               string sym = tradesMap2[i].symbol;
               double margin = SymbolInfoDouble(sym, SYMBOL_MARGIN_INITIAL);
               if(margin > 0) {
                  risk = margin * tradesMap2[i].volume;
               } else {
                  double contractSize = SymbolInfoDouble(sym, SYMBOL_TRADE_CONTRACT_SIZE);
                  if(contractSize > 0) {
                     risk = tradesMap2[i].openPrice * tradesMap2[i].volume * contractSize * 0.02;
                  }
               }
            }
            
            // Calculate balance at open and risk percent
            datetime openTime2 = tradesMap2[i].openTime;
            double balanceAtOpen2 = AccountInfoDouble(ACCOUNT_BALANCE);
            if(HistorySelect(openTime2, nowdt5)) {
               int tot5 = HistoryDealsTotal();
               for(int k=0;k<tot5;k++) {
                  ulong deal5 = HistoryDealGetTicket(k);
                  if(deal5<=0) continue;
                  long ts5 = (long)HistoryDealGetInteger(deal5, DEAL_TIME);
                  if(ts5 <= openTime2) continue;
                  
                  int dtype5 = (int)HistoryDealGetInteger(deal5, DEAL_TYPE);
                  int entry5 = (int)HistoryDealGetInteger(deal5, DEAL_ENTRY);
                  double pr5 = HistoryDealGetDouble(deal5, DEAL_PROFIT);
                  
                  bool affectBal = (dtype5 == DEAL_TYPE_BALANCE || dtype5 == DEAL_TYPE_CREDIT || dtype5 == DEAL_TYPE_CHARGE
                                   || dtype5 == DEAL_TYPE_BONUS || dtype5 == DEAL_TYPE_COMMISSION || dtype5 == DEAL_TYPE_COMMISSION_DAILY
                                   || dtype5 == DEAL_TYPE_COMMISSION_MONTHLY || dtype5 == DEAL_TYPE_INTEREST
                                   || ((dtype5 == DEAL_TYPE_BUY || dtype5 == DEAL_TYPE_SELL) && entry5 == DEAL_ENTRY_OUT));
                  if(affectBal) {
                     balanceAtOpen2 -= pr5;
                  }
               }
            }
            
            double riskPercent2 = 0.0;
            if(balanceAtOpen2 > 0 && risk > 0) {
               riskPercent2 = (risk / balanceAtOpen2) * 100.0;
            }
            
            tradesMap2[i].risk = risk;
            tradesMap2[i].riskPercent = riskPercent2;
            
            // Find or create symbol stats entry
            string sym2 = tradesMap2[i].symbol;
            int statIdx = -1;
            for(int j=0;j<statsSize;j++) {
               if(statsMap[j].symbol == sym2) {
                  statIdx = j;
                  break;
               }
            }
            
            if(statIdx < 0) {
               ArrayResize(statsMap, statsSize+1);
               statIdx = statsSize;
               statsMap[statIdx].symbol = sym2;
               statsMap[statIdx].totalTrades = 0;
               statsMap[statIdx].profitTrades = 0;
               statsMap[statIdx].lossTrades = 0;
               statsMap[statIdx].totalProfit = 0.0;
               statsMap[statIdx].totalLoss = 0.0;
               statsMap[statIdx].grossProfit = 0.0;
               statsMap[statIdx].grossLoss = 0.0;
               statsMap[statIdx].netProfit = 0.0;
               statsMap[statIdx].totalCommission = 0.0;
               statsMap[statIdx].totalSwap = 0.0;
               statsMap[statIdx].totalRisk = 0.0;
               statsMap[statIdx].largestProfit = 0.0;
               statsMap[statIdx].largestLoss = 0.0;
               statsMap[statIdx].averageProfit = 0.0;
               statsMap[statIdx].averageLoss = 0.0;
               statsMap[statIdx].winRate = 0.0;
               statsMap[statIdx].profitFactor = 0.0;
               statsMap[statIdx].avgRiskPercent = 0.0;
               statsSize++;
            }
            
            // Update statistics
            statsMap[statIdx].totalTrades++;
            statsMap[statIdx].totalCommission += tradesMap2[i].commission;
            statsMap[statIdx].totalSwap += tradesMap2[i].swap;
            statsMap[statIdx].totalRisk += tradesMap2[i].risk;
            
            double tradeProfit = tradesMap2[i].profit;
            statsMap[statIdx].netProfit += tradeProfit;
            
            if(tradeProfit > 0) {
               statsMap[statIdx].profitTrades++;
               statsMap[statIdx].grossProfit += tradeProfit;
               if(tradeProfit > statsMap[statIdx].largestProfit) {
                  statsMap[statIdx].largestProfit = tradeProfit;
               }
            } else if(tradeProfit < 0) {
               statsMap[statIdx].lossTrades++;
               statsMap[statIdx].grossLoss += tradeProfit; // negative value
               if(tradeProfit < statsMap[statIdx].largestLoss) {
                  statsMap[statIdx].largestLoss = tradeProfit;
               }
            }
            
            // Accumulate risk percent for average
            statsMap[statIdx].avgRiskPercent += tradesMap2[i].riskPercent;
         }
         
         // Calculate final statistics and build response
         CJAVal symbols;
         for(int i=0;i<statsSize;i++) {
            if(statsMap[i].totalTrades == 0) continue;
            
            // Calculate averages and ratios
            if(statsMap[i].profitTrades > 0) {
               statsMap[i].averageProfit = statsMap[i].grossProfit / statsMap[i].profitTrades;
            }
            if(statsMap[i].lossTrades > 0) {
               statsMap[i].averageLoss = statsMap[i].grossLoss / statsMap[i].lossTrades;
            }
            statsMap[i].winRate = (statsMap[i].profitTrades / (double)statsMap[i].totalTrades) * 100.0;
            statsMap[i].avgRiskPercent = statsMap[i].avgRiskPercent / statsMap[i].totalTrades;
            
            if(MathAbs(statsMap[i].grossLoss) > 0.0001) {
               statsMap[i].profitFactor = MathAbs(statsMap[i].grossProfit / statsMap[i].grossLoss);
            }
            
            CJAVal stat;
            stat["symbol"] = statsMap[i].symbol;
            stat["total_trades"] = statsMap[i].totalTrades;
            stat["profit_trades"] = statsMap[i].profitTrades;
            stat["loss_trades"] = statsMap[i].lossTrades;
            stat["win_rate"] = statsMap[i].winRate;
            stat["total_profit"] = statsMap[i].netProfit;
            stat["gross_profit"] = statsMap[i].grossProfit;
            stat["gross_loss"] = statsMap[i].grossLoss;
            stat["net_profit"] = statsMap[i].netProfit;
            stat["profit_factor"] = statsMap[i].profitFactor;
            stat["total_commission"] = statsMap[i].totalCommission;
            stat["total_swap"] = statsMap[i].totalSwap;
            stat["total_risk"] = statsMap[i].totalRisk;
            stat["average_risk_percent"] = statsMap[i].avgRiskPercent;
            stat["largest_profit"] = statsMap[i].largestProfit;
            stat["largest_loss"] = statsMap[i].largestLoss;
            stat["average_profit"] = statsMap[i].averageProfit;
            stat["average_loss"] = statsMap[i].averageLoss;
            symbols.Add(stat);
         }
         
         resp["data"] = symbols;
      }
   } else
   if(action == "get_trades_minimal") {
      CJAVal *qp9 = query["params"];
      long fromTs = 0;
      long toTs = 0;
      if(CheckPointer(qp9)!=POINTER_INVALID) {
         CJAVal *vf9 = (*qp9)["from"];
         CJAVal *vt9 = (*qp9)["to"];
         if(CheckPointer(vf9)!=POINTER_INVALID) fromTs = vf9.ToInt();
         if(CheckPointer(vt9)!=POINTER_INVALID) toTs = vt9.ToInt();
      }
      datetime from = (fromTs>0 ? (datetime)fromTs : (datetime)(TimeCurrent() - 30*24*60*60));
      datetime to = (toTs>0 ? (datetime)toTs : TimeCurrent());
      
      if(!HistorySelect(from,to)) {
         resp["ok"] = false;
         resp["error"] = "HistorySelect failed";
      } else {
         // Build minimal trade data structure for storage
         struct MinimalTrade {
            ulong positionId;
            ulong orderTicket;
            datetime openTime;
            datetime closeTime;
            double openPrice;
            double closePrice;
            double stopLoss;
            double takeProfit;
            string symbol;
            int type;
            double volume;
            double profit;
            double commission;
            double swap;
            double risk;
            double riskPercent;
            double balanceAtOpen;
            string comment;
            int dayOfWeek;
            int hourOfDay;
            int month;
         };
         
         MinimalTrade trades[];
         int tradesSize = 0;
         
         // First, group deals by position (same as get_trade_history)
         struct TradeData3 {
            ulong positionId;
            ulong orderTicket;
            datetime openTime;
            datetime closeTime;
            int type;
            string symbol;
            double volume;
            double openPrice;
            double closePrice;
            double commission;
            double swap;
            double profit;
         };
         
         TradeData3 tradesMap3[];
         int mapSize3 = 0;
         
         int dtotal4 = HistoryDealsTotal();
         for(int i=0;i<dtotal4;i++) {
            ulong dticket4 = HistoryDealGetTicket(i);
            if(dticket4<=0) continue;
            
            ulong posId = HistoryDealGetInteger(dticket4, DEAL_POSITION_ID);
            if(posId == 0) continue;
            
            int dtype4 = (int)HistoryDealGetInteger(dticket4, DEAL_TYPE);
            int entry4 = (int)HistoryDealGetInteger(dticket4, DEAL_ENTRY);
            
            int tradeIdx = -1;
            for(int j=0;j<mapSize3;j++) {
               if(tradesMap3[j].positionId == posId) {
                  tradeIdx = j;
                  break;
               }
            }
            
            if(tradeIdx < 0) {
               ArrayResize(tradesMap3, mapSize3+1);
               tradeIdx = mapSize3;
               tradesMap3[tradeIdx].positionId = posId;
               tradesMap3[tradeIdx].orderTicket = HistoryDealGetInteger(dticket4, DEAL_ORDER);
               tradesMap3[tradeIdx].symbol = HistoryDealGetString(dticket4, DEAL_SYMBOL);
               tradesMap3[tradeIdx].volume = HistoryDealGetDouble(dticket4, DEAL_VOLUME);
               tradesMap3[tradeIdx].commission = 0.0;
               tradesMap3[tradeIdx].swap = 0.0;
               tradesMap3[tradeIdx].profit = 0.0;
               mapSize3++;
            }
            
            double dealSwap = HistoryDealGetDouble(dticket4, DEAL_SWAP);
            if(dealSwap != 0.0) {
               tradesMap3[tradeIdx].swap += dealSwap;
            }
            
            if(dtype4 == DEAL_TYPE_BUY || dtype4 == DEAL_TYPE_SELL) {
               if(entry4 == DEAL_ENTRY_IN) {
                  tradesMap3[tradeIdx].openTime = (datetime)HistoryDealGetInteger(dticket4, DEAL_TIME);
                  tradesMap3[tradeIdx].openPrice = HistoryDealGetDouble(dticket4, DEAL_PRICE);
                  tradesMap3[tradeIdx].type = dtype4;
                  if(tradesMap3[tradeIdx].orderTicket == 0) {
                     tradesMap3[tradeIdx].orderTicket = HistoryDealGetInteger(dticket4, DEAL_ORDER);
                  }
               } else if(entry4 == DEAL_ENTRY_OUT) {
                  tradesMap3[tradeIdx].closeTime = (datetime)HistoryDealGetInteger(dticket4, DEAL_TIME);
                  tradesMap3[tradeIdx].closePrice = HistoryDealGetDouble(dticket4, DEAL_PRICE);
                  tradesMap3[tradeIdx].profit += HistoryDealGetDouble(dticket4, DEAL_PROFIT);
               }
            } else if(dtype4 == DEAL_TYPE_COMMISSION || dtype4 == DEAL_TYPE_COMMISSION_DAILY || dtype4 == DEAL_TYPE_COMMISSION_MONTHLY) {
               tradesMap3[tradeIdx].commission += HistoryDealGetDouble(dticket4, DEAL_PROFIT);
            }
         }
         
         // Build minimal data for each closed trade
         datetime nowdt6 = TimeCurrent();
         for(int i=0;i<mapSize3;i++) {
            if(tradesMap3[i].closeTime == 0) continue; // Skip open positions
            
            ArrayResize(trades, tradesSize+1);
            
            trades[tradesSize].positionId = tradesMap3[i].positionId;
            trades[tradesSize].orderTicket = tradesMap3[i].orderTicket;
            trades[tradesSize].openTime = tradesMap3[i].openTime;
            trades[tradesSize].closeTime = tradesMap3[i].closeTime;
            trades[tradesSize].openPrice = tradesMap3[i].openPrice;
            trades[tradesSize].closePrice = tradesMap3[i].closePrice;
            trades[tradesSize].symbol = tradesMap3[i].symbol;
            trades[tradesSize].type = tradesMap3[i].type;
            trades[tradesSize].volume = tradesMap3[i].volume;
            trades[tradesSize].profit = tradesMap3[i].profit;
            trades[tradesSize].commission = tradesMap3[i].commission;
            trades[tradesSize].swap = tradesMap3[i].swap;
            trades[tradesSize].stopLoss = 0.0;
            trades[tradesSize].takeProfit = 0.0;
            trades[tradesSize].risk = 0.0;
            trades[tradesSize].riskPercent = 0.0;
            trades[tradesSize].comment = "";
            
            // Get SL/TP from order
            if(trades[tradesSize].orderTicket > 0 && HistoryOrderSelect(trades[tradesSize].orderTicket)) {
               trades[tradesSize].stopLoss = HistoryOrderGetDouble(trades[tradesSize].orderTicket, ORDER_SL);
               trades[tradesSize].takeProfit = HistoryOrderGetDouble(trades[tradesSize].orderTicket, ORDER_TP);
               trades[tradesSize].comment = HistoryOrderGetString(trades[tradesSize].orderTicket, ORDER_COMMENT);
            }
            
            // Calculate risk
            if(trades[tradesSize].stopLoss > 0) {
               string sym = trades[tradesSize].symbol;
               double point = SymbolInfoDouble(sym, SYMBOL_POINT);
               double tickValue = SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_VALUE);
               double tickSize = SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_SIZE);
               
               if(tickSize > 0 && tickValue > 0) {
                  double priceDiff = MathAbs(trades[tradesSize].openPrice - trades[tradesSize].stopLoss);
                  double ticks = priceDiff / tickSize;
                  trades[tradesSize].risk = ticks * tickValue * trades[tradesSize].volume;
               } else {
                  double contractSize = SymbolInfoDouble(sym, SYMBOL_TRADE_CONTRACT_SIZE);
                  if(contractSize > 0) {
                     trades[tradesSize].risk = MathAbs(trades[tradesSize].openPrice - trades[tradesSize].stopLoss) * trades[tradesSize].volume * contractSize / point;
                  }
               }
            }
            
            if(trades[tradesSize].risk == 0.0) {
               string sym = trades[tradesSize].symbol;
               double margin = SymbolInfoDouble(sym, SYMBOL_MARGIN_INITIAL);
               if(margin > 0) {
                  trades[tradesSize].risk = margin * trades[tradesSize].volume;
               } else {
                  double contractSize = SymbolInfoDouble(sym, SYMBOL_TRADE_CONTRACT_SIZE);
                  if(contractSize > 0) {
                     trades[tradesSize].risk = trades[tradesSize].openPrice * trades[tradesSize].volume * contractSize * 0.02;
                  }
               }
            }
            
            // Calculate balance at open time
            trades[tradesSize].balanceAtOpen = AccountInfoDouble(ACCOUNT_BALANCE);
            if(HistorySelect(trades[tradesSize].openTime, nowdt6)) {
               int tot6 = HistoryDealsTotal();
               for(int k=0;k<tot6;k++) {
                  ulong deal6 = HistoryDealGetTicket(k);
                  if(deal6<=0) continue;
                  long ts6 = (long)HistoryDealGetInteger(deal6, DEAL_TIME);
                  if(ts6 <= trades[tradesSize].openTime) continue;
                  
                  int dtype6 = (int)HistoryDealGetInteger(deal6, DEAL_TYPE);
                  int entry6 = (int)HistoryDealGetInteger(deal6, DEAL_ENTRY);
                  double pr6 = HistoryDealGetDouble(deal6, DEAL_PROFIT);
                  
                  bool affectBal = (dtype6 == DEAL_TYPE_BALANCE || dtype6 == DEAL_TYPE_CREDIT || dtype6 == DEAL_TYPE_CHARGE
                                   || dtype6 == DEAL_TYPE_BONUS || dtype6 == DEAL_TYPE_COMMISSION || dtype6 == DEAL_TYPE_COMMISSION_DAILY
                                   || dtype6 == DEAL_TYPE_COMMISSION_MONTHLY || dtype6 == DEAL_TYPE_INTEREST
                                   || ((dtype6 == DEAL_TYPE_BUY || dtype6 == DEAL_TYPE_SELL) && entry6 == DEAL_ENTRY_OUT));
                  if(affectBal) {
                     trades[tradesSize].balanceAtOpen -= pr6;
                  }
               }
            }
            
            // Calculate risk percent
            if(trades[tradesSize].balanceAtOpen > 0 && trades[tradesSize].risk > 0) {
               trades[tradesSize].riskPercent = (trades[tradesSize].risk / trades[tradesSize].balanceAtOpen) * 100.0;
            }
            
            // Extract time components
            MqlDateTime dtOpen;
            TimeToStruct(trades[tradesSize].openTime, dtOpen);
            trades[tradesSize].dayOfWeek = dtOpen.day_of_week; // 0=Sunday, 1=Monday, etc.
            trades[tradesSize].hourOfDay = dtOpen.hour;
            trades[tradesSize].month = dtOpen.mon;
            
            tradesSize++;
         }
         
         // Build JSON response
         CJAVal tradesArray;
         for(int i=0;i<tradesSize;i++) {
            CJAVal t;
            t["position_id"] = (long)trades[i].positionId;
            t["order_ticket"] = (long)trades[i].orderTicket;
            t["open_time"] = (long)trades[i].openTime;
            t["close_time"] = (long)trades[i].closeTime;
            t["open_price"] = trades[i].openPrice;
            t["close_price"] = trades[i].closePrice;
            t["stop_loss"] = trades[i].stopLoss;
            t["take_profit"] = trades[i].takeProfit;
            t["symbol"] = trades[i].symbol;
            t["type"] = (trades[i].type == DEAL_TYPE_BUY ? "BUY" : "SELL");
            t["volume"] = trades[i].volume;
            t["profit"] = trades[i].profit;
            t["commission"] = trades[i].commission;
            t["swap"] = trades[i].swap;
            t["risk"] = trades[i].risk;
            t["risk_percent"] = trades[i].riskPercent;
            t["balance_at_open"] = trades[i].balanceAtOpen;
            t["comment"] = trades[i].comment;
            t["day_of_week"] = trades[i].dayOfWeek;
            t["hour_of_day"] = trades[i].hourOfDay;
            t["month"] = trades[i].month;
            tradesArray.Add(t);
         }
         
         resp["data"] = tradesArray;
      }
   } else
   if(action == "set_rules") {
      // Set marathon validation rules
      CJAVal *rulesPtr = query["params"]["rules"];
      if(CheckPointer(rulesPtr) != POINTER_INVALID && (*rulesPtr).m_type == jtARRAY) {
         SetRules(rulesPtr);
         CJAVal data;
         data["message"] = "Rules set successfully";
         resp["data"] = data;
         Print("✅ Rules set successfully");
      } else {
         resp["ok"] = false;
         resp["error"] = "Invalid rules format";
         Print("⚠️ Invalid rules format");
      }
   } else
   if(action == "get_rules_status") {
      // Get current rules status
      CJAVal status;
      GetRulesStatus(status);
      resp["data"] = status;
      Print("✅ Rules status retrieved");
   } else
   if(action == "set_marathon_start_balance") {
      // Set marathon start balance for total drawdown calculation
      CJAVal *paramsPtr = query["params"];
      double balance = 0.0;

      if(CheckPointer(paramsPtr) != POINTER_INVALID) {
         CJAVal *balPtr = (*paramsPtr)["balance"];
         if(CheckPointer(balPtr) != POINTER_INVALID) {
            balance = balPtr.ToDbl();
         }
      }

      // If balance not provided, use current balance
      if(balance <= 0.0) {
         balance = AccountInfoDouble(ACCOUNT_BALANCE);
      }

      marathonStartBalance = balance;

      CJAVal data;
      data["message"] = "Marathon start balance set successfully";
      data["balance"] = balance;
      resp["data"] = data;

      Print("✅ Marathon start balance set to ", DoubleToString(balance, 2));
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

//+------------------------------------------------------------------+
//| Set marathon validation rules                                    |
//+------------------------------------------------------------------+
void SetRules(CJAVal &rulesArray) {
   // Reset rules
   maxDailyDrawdownPercent = 0.0;
   maxTotalDrawdownPercent = 0.0;
   floatingRiskPercent = 0.0;
   rulesInitialized = false;
   failedRules = "";
   failedReasons = "";

   int rulesCount = rulesArray.Size();
   for(int i = 0; i < rulesCount; i++) {
      CJAVal rule = rulesArray[i];
      string ruleType = rule["type"].ToStr();
      double limit = rule["limit"].ToDbl();

      if(ruleType == "MAX_DAILY_DRAWDOWN_PERCENT") {
         maxDailyDrawdownPercent = limit;
      } else if(ruleType == "MAX_TOTAL_DRAWDOWN_PERCENT") {
         maxTotalDrawdownPercent = limit;
      } else if(ruleType == "FLOATING_RISK_PERCENT") {
         floatingRiskPercent = limit;
      }
   }

   if(maxDailyDrawdownPercent > 0.0 || maxTotalDrawdownPercent > 0.0 || floatingRiskPercent > 0.0) {
      rulesInitialized = true;
      Print("📋 Rules set: daily_dd=", DoubleToString(maxDailyDrawdownPercent, 2),
            "%, total_dd=", DoubleToString(maxTotalDrawdownPercent, 2),
            "%, floating_risk=", DoubleToString(floatingRiskPercent, 2), "%");
   }
}

//+------------------------------------------------------------------+
//| Get current rules status                                         |
//+------------------------------------------------------------------+
void GetRulesStatus(CJAVal &status) {
   // Status is already checked every tick, just return current values
   status["passed"] = (StringLen(failedRules) == 0);
   status["failed_rules"];
   status["failed_reasons"];
   status["last_check"] = (long)lastRuleCheckTime;
   status["total_rules"] = (rulesInitialized ? 1 : 0); // Simplified to 1 if any rules set

   // Parse failed rules and reasons
   if(StringLen(failedRules) > 0) {
      string failedRulesArray[];
      string failedReasonsArray[];

      StringSplit(failedRules, ',', failedRulesArray);
      StringSplit(failedReasons, ',', failedReasonsArray);

      for(int i = 0; i < ArraySize(failedRulesArray); i++) {
         StringTrimLeft(failedRulesArray[i]);
         StringTrimRight(failedRulesArray[i]);
         status["failed_rules"].Add(failedRulesArray[i]);
      }

      for(int i = 0; i < ArraySize(failedReasonsArray); i++) {
         StringTrimLeft(failedReasonsArray[i]);
         StringTrimRight(failedReasonsArray[i]);
         status["failed_reasons"].Add(failedReasonsArray[i]);
      }
   }
}

//+------------------------------------------------------------------+
//| Check current rules status and update failure tracking          |
//+------------------------------------------------------------------+
void CheckRulesStatus() {
   if(!rulesInitialized) {
      failedRules = "";
      failedReasons = "";
      rulesWerePassing = true;
      lastRuleCheckTime = TimeCurrent();
      return;
   }

   string oldFailedRules = failedRules;
   failedRules = "";
   failedReasons = "";
   bool hasFailures = false;

   // Check daily drawdown
   if(maxDailyDrawdownPercent > 0.0) {
      double dailyDD = GetCurrentDailyDrawdownPercent();
      if(dailyDD >= maxDailyDrawdownPercent) {
         failedRules += (StringLen(failedRules) > 0 ? "," : "") + "MAX_DAILY_DRAWDOWN_PERCENT";
         failedReasons += (StringLen(failedReasons) > 0 ? "," : "") + GetRuleFailureReason("MAX_DAILY_DRAWDOWN_PERCENT", dailyDD, maxDailyDrawdownPercent);
         hasFailures = true;
      }
   }

   // Check total drawdown
   if(maxTotalDrawdownPercent > 0.0) {
      double totalDD = GetCurrentTotalDrawdownPercent();
      if(totalDD >= maxTotalDrawdownPercent) {
         failedRules += (StringLen(failedRules) > 0 ? "," : "") + "MAX_TOTAL_DRAWDOWN_PERCENT";
         failedReasons += (StringLen(failedReasons) > 0 ? "," : "") + GetRuleFailureReason("MAX_TOTAL_DRAWDOWN_PERCENT", totalDD, maxTotalDrawdownPercent);
         hasFailures = true;
      }
   }

   // Check floating risk
   if(floatingRiskPercent > 0.0) {
      double floatingRisk = GetCurrentFloatingRiskPercent();
      if(floatingRisk >= floatingRiskPercent) {
         failedRules += (StringLen(failedRules) > 0 ? "," : "") + "FLOATING_RISK_PERCENT";
         failedReasons += (StringLen(failedReasons) > 0 ? "," : "") + GetRuleFailureReason("FLOATING_RISK_PERCENT", floatingRisk, floatingRiskPercent);
         hasFailures = true;
      }
   }

   lastRuleCheckTime = TimeCurrent();

   // Check if rules transitioned from passing to failing
   bool rulesCurrentlyPassing = !hasFailures;
   if(rulesWerePassing && !rulesCurrentlyPassing && oldFailedRules != failedRules) {
      // Rules just started failing - send violation alert
      SendRuleViolationAlert(failedRules, failedReasons);
   }

   rulesWerePassing = rulesCurrentlyPassing;

   if(!hasFailures) {
      // Only log when rules start passing again or for periodic confirmation
      static datetime lastPassLog = 0;
      if(TimeCurrent() - lastPassLog > 300) { // Log every 5 minutes when passing
         Print("✅ All rules passed");
         lastPassLog = TimeCurrent();
      }
   } else {
      // Always log failures
      Print("⚠️ Rules failed: ", failedRules);
   }
}

//+------------------------------------------------------------------+
//| Get rule failure reason string                                   |
//+------------------------------------------------------------------+
string GetRuleFailureReason(string ruleType, double currentValue, double limit) {
   return StringFormat("%.2f%% exceeds limit of %.2f%%", currentValue, limit);
}

//+------------------------------------------------------------------+
//| Get current daily drawdown percentage                            |
//+------------------------------------------------------------------+
double GetCurrentDailyDrawdownPercent() {
   string currentLogin = (string)AccountInfoInteger(ACCOUNT_LOGIN);
   MqlDateTime dt;
   TimeCurrent(dt);
   int currentDate = dt.year * 10000 + dt.mon * 100 + dt.day;

   double dailyStartBalance = GetDailyStartBalance(currentLogin, currentDate);
   double currentBalance = AccountInfoDouble(ACCOUNT_BALANCE);

   if(dailyStartBalance <= 0.0) {
      return 0.0;
   }

   double drawdown = dailyStartBalance - currentBalance;
   return (drawdown / dailyStartBalance) * 100.0;
}

//+------------------------------------------------------------------+
//| Get current total drawdown percentage                            |
//+------------------------------------------------------------------+
double GetCurrentTotalDrawdownPercent() {
   double startBalance = marathonStartBalance;
   if(startBalance <= 0.0) {
      // If marathon start balance not set, use account balance at initialization
      // For simplicity, we'll use current balance as reference (no historical drawdown)
      return 0.0;
   }

   double currentBalance = AccountInfoDouble(ACCOUNT_BALANCE);
   double drawdown = startBalance - currentBalance;

   return (drawdown / startBalance) * 100.0;
}

//+------------------------------------------------------------------+
//| Get current floating risk percentage                             |
//+------------------------------------------------------------------+
double GetCurrentFloatingRiskPercent() {
   // Calculate risk as margin used / equity
   double margin = AccountInfoDouble(ACCOUNT_MARGIN);
   double equity = AccountInfoDouble(ACCOUNT_EQUITY);

   if(equity <= 0.0) {
      return 0.0;
   }

   return (margin / equity) * 100.0;
}

//+------------------------------------------------------------------+
//| Send rule violation alert to server                              |
//+------------------------------------------------------------------+
void SendRuleViolationAlert(string alertFailedRules, string alertFailedReasons) {
   if(!IsSocketConnected()) return;

   CJAVal alert;
   alert["type"] = "rule_violation";
   alert["login"] = (string)AccountInfoInteger(ACCOUNT_LOGIN);
   alert["timestamp"] = GetCurrentTimestamp();
   alert["failed_rules"] = alertFailedRules;
   alert["failed_reasons"] = alertFailedReasons;
   alert["balance"] = AccountInfoDouble(ACCOUNT_BALANCE);
   alert["equity"] = AccountInfoDouble(ACCOUNT_EQUITY);
   alert["profit"] = AccountInfoDouble(ACCOUNT_PROFIT);
   alert["margin"] = AccountInfoDouble(ACCOUNT_MARGIN);

   // Add current rule values for context
   if(maxDailyDrawdownPercent > 0.0) {
      alert["current_daily_drawdown"] = GetCurrentDailyDrawdownPercent();
      alert["max_daily_drawdown_limit"] = maxDailyDrawdownPercent;
   }
   if(maxTotalDrawdownPercent > 0.0) {
      alert["current_total_drawdown"] = GetCurrentTotalDrawdownPercent();
      alert["max_total_drawdown_limit"] = maxTotalDrawdownPercent;
   }
   if(floatingRiskPercent > 0.0) {
      alert["current_floating_risk"] = GetCurrentFloatingRiskPercent();
      alert["floating_risk_limit"] = floatingRiskPercent;
   }

   SendData(alert.Serialize());
   Print("🚨 Rule violation alert sent: ", alertFailedRules);
}