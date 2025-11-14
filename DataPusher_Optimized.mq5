//+------------------------------------------------------------------+
//|                                          DataPusher_Optimized.mq5 |
//|                                   Optimized for Marathon RabbitMQ |
//|                                  Implements optimization strategy |
//+------------------------------------------------------------------+
#include "/socketlib.mqh"
#include <Trade/Trade.mqh>
#include <JAson.mqh>

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
   if(expertStatus == STATUS_DISCONNECTED || expertStatus == STATUS_ERROR) {
      if(int(TimeCurrent() - lastConnectionAttempt) >= reconnectDelay) {
         Print("Attempting to reconnect...");
         expertStatus = STATUS_RECONNECTING;
         ConnectSocket();
      }
      return;
   }
   
   if(!IsSocketConnected()) return;
   
   datetime now = TimeCurrent();
   
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
   
   // 4. Check for equity changes (send quick update every 5 seconds if changed)
   if(int(now - lastUpdateSent) >= UPDATE_INTERVAL) {
      if(HasEquityChanged()) {
         SendQuickUpdate();
         lastUpdateSent = now;
      }
   }
}

//+------------------------------------------------------------------+
//| Socket connection function                                         |
//+------------------------------------------------------------------+
void ConnectSocket() {
   if(client == INVALID_SOCKET64) {
      lastConnectionAttempt = TimeCurrent();
      
      char wsaData[]; ArrayResize(wsaData,sizeof(WSAData));
      int res = WSAStartup(MAKEWORD(2,2),wsaData);
      if(res != 0) {
         ProcessSocketError(__FUNCTION__, res);
         return;
      }

      client = socket(AF_INET,SOCK_STREAM,IPPROTO_TCP);
      if(client == INVALID_SOCKET64) {
         ProcessSocketError(__FUNCTION__, WSAGetLastError());
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
         if(err != WSAEISCONN) {
            ProcessSocketError(__FUNCTION__, err);
            return;
         }
      }

      int non_block=1;
      res = ioctlsocket(client,(int)FIONBIO,non_block);
      if(res != NO_ERROR) {
         ProcessSocketError(__FUNCTION__, res);
         return;
      }

      expertStatus = STATUS_CONNECTED;
      Print(__FUNCTION__," > socket created and connected successfully");
   }
}

//+------------------------------------------------------------------+
//| Clean socket closure function                                      |
//+------------------------------------------------------------------+
void CloseClean() {
   if(client != INVALID_SOCKET64) {
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
   
   // Log the data being sent (first 200 chars to avoid spam)
   string preview = StringSubstr(data, 0, 200);
   if(StringLen(data) > 200) {
      preview += "...";
   }
   Print("📤 Sending to Tokyo: ", preview);
   
   // Add message terminator
   data = data + "\n";
   
   uchar dataArray[];
   StringToCharArray(data, dataArray);
   
   int bytesSent = send(client, dataArray, ArraySize(dataArray)-1, 0);
   if(bytesSent == SOCKET_ERROR) {
      ProcessSocketError(__FUNCTION__, WSAGetLastError());
   } else {
      Print("✅ Sent ", bytesSent, " bytes successfully");
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
   return client != INVALID_SOCKET64;
}

