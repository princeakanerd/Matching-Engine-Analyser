// src/bot_fleet.rs
// Simulates a fleet of algorithmic trading bots flooding the contestant's engine over TCP.

use rand::Rng;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::sleep;

/// Represents the type of order sent to the engine.
#[derive(Debug)]
pub enum OrderType {
    Limit,
    Market,
}

/// Represents an Order that will be serialized and sent to the engine.
#[derive(Debug)]
pub struct Order {
    pub order_id: u64,
    pub symbol: String,
    pub side: String, // "BUY" or "SELL"
    pub order_type: OrderType,
    pub price: f64,
    pub quantity: u32,
}

impl Order {
    /// Serializes the order to a simple CSV string (easy for contestants to parse).
    /// Format: OrderID,Symbol,Side,Type,Price,Quantity
    pub fn to_csv(&self) -> String {
        let type_str = match self.order_type {
            OrderType::Limit => "LIMIT",
            OrderType::Market => "MARKET",
        };
        format!(
            "{},{},{},{},{:.2},{}\n",
            self.order_id, self.symbol, self.side, type_str, self.price, self.quantity
        )
    }
}

/// Generates a random order.
fn generate_random_order(id: u64) -> Order {
    let mut rng = rand::thread_rng();
    let symbols = ["AAPL", "GOOGL", "TSLA", "MSFT"];
    let is_buy: bool = rng.gen();
    let is_limit: bool = rng.gen_bool(0.8); // 80% chance of Limit order

    Order {
        order_id: id,
        symbol: symbols[rng.gen_range(0..symbols.len())].to_string(),
        side: if is_buy { "BUY".to_string() } else { "SELL".to_string() },
        order_type: if is_limit { OrderType::Limit } else { OrderType::Market },
        price: rng.gen_range(100.0_f64..500.0_f64),
        quantity: rng.gen_range(1..100),
    }
}

/// Spawns a single bot that connects to the engine and fires orders.
async fn run_bot(bot_id: u32, target_ip: String, target_port: u16, num_orders: u64) {
    let address = format!("{}:{}", target_ip, target_port);
    
    // Wait a brief moment for the container's engine to fully boot up and bind to the port
    sleep(Duration::from_millis(1000)).await;

    let mut stream = match TcpStream::connect(&address).await {
        Ok(s) => s,
        Err(e) => {
            log::error!("[Bot {}] Failed to connect to engine at {}: {}", bot_id, address, e);
            return;
        }
    };

    log::info!("[Bot {}] Connected! Flooding engine with {} orders...", bot_id, num_orders);

    let mut success_count = 0;
    for i in 0..num_orders {
        let order = generate_random_order((bot_id as u64 * 1000000) + i);
        let payload = order.to_csv();

        if let Err(e) = stream.write_all(payload.as_bytes()).await {
            log::error!("[Bot {}] Connection dropped by engine on order {}: {}", bot_id, i, e);
            break;
        }
        success_count += 1;
    }
    
    log::info!("[Bot {}] Finished sending {}/{} orders.", bot_id, success_count, num_orders);
}

/// Launches the entire bot fleet against the container.
pub async fn launch_bot_fleet(target_port: u16) {
    let num_bots = 10;
    let orders_per_bot: u64 = 10_000;
    let mut handles = vec![];

    log::info!("🚀 Launching Bot Fleet: {} bots, {} total orders on port {}", num_bots, num_bots as u64 * orders_per_bot, target_port);

    for bot_id in 0..num_bots {
        let handle = tokio::spawn(async move {
            run_bot(bot_id, "127.0.0.1".to_string(), target_port, orders_per_bot).await;
        });
        handles.push(handle);
    }

    // Wait for all bots to finish sending their data
    for handle in handles {
        let _ = handle.await;
    }
    
    log::info!("🏁 Bot Fleet attack complete.");
}
