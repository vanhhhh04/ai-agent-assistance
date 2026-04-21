CREATE TABLE customers (
  id SERIAL PRIMARY KEY,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  phone TEXT,
  gender TEXT CHECK(gender IN ('male','female','non_binary')),
  date_of_birth DATE,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE addresses (
  id SERIAL PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  type TEXT NOT NULL CHECK(type IN ('billing','shipping')),
  street TEXT NOT NULL,
  city TEXT NOT NULL,
  state TEXT NOT NULL,
  zip_code TEXT NOT NULL,
  country TEXT NOT NULL DEFAULT 'US',
  is_default BOOLEAN NOT NULL DEFAULT FALSE, -- Changed from 0 to FALSE
  created_at TIMESTAMP NOT NULL,
  FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE categories (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  description TEXT,
  parent_category_id INTEGER,
  created_at TIMESTAMP NOT NULL,
  FOREIGN KEY (parent_category_id) REFERENCES categories(id)
);

CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  category_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  description TEXT,
  sku TEXT NOT NULL UNIQUE,
  price NUMERIC(12,2) NOT NULL, -- Changed REAL to NUMERIC for accuracy
  cost NUMERIC(12,2) NOT NULL,
  stock_quantity INTEGER NOT NULL DEFAULT 0,
  brand TEXT NOT NULL,
  weight REAL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE, -- Changed from 1 to TRUE
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  FOREIGN KEY (category_id) REFERENCES categories(id)
);

CREATE TABLE coupons (
  id SERIAL PRIMARY KEY,
  code TEXT NOT NULL UNIQUE,
  discount_type TEXT NOT NULL CHECK(discount_type IN ('percentage','fixed')),
  discount_value NUMERIC(12,2) NOT NULL,
  min_order_amount NUMERIC(12,2) DEFAULT 0,
  max_uses INTEGER,
  times_used INTEGER NOT NULL DEFAULT 0,
  valid_from DATE NOT NULL,
  valid_until DATE NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMP NOT NULL
);

CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  shipping_address_id INTEGER NOT NULL,
  billing_address_id INTEGER NOT NULL,
  coupon_id INTEGER,
  status TEXT NOT NULL CHECK(status IN ('pending','processing','shipped','delivered','cancelled','returned')),
  subtotal NUMERIC(12,2) NOT NULL,
  discount_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
  tax_amount NUMERIC(12,2) NOT NULL,
  shipping_cost NUMERIC(12,2) NOT NULL,
  total_amount NUMERIC(12,2) NOT NULL,
  order_date TIMESTAMP NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  FOREIGN KEY (customer_id) REFERENCES customers(id),
  FOREIGN KEY (shipping_address_id) REFERENCES addresses(id),
  FOREIGN KEY (billing_address_id) REFERENCES addresses(id),
  FOREIGN KEY (coupon_id) REFERENCES coupons(id)
);

CREATE TABLE order_items (
  id SERIAL PRIMARY KEY,
  order_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  quantity INTEGER NOT NULL,
  unit_price NUMERIC(12,2) NOT NULL,
  total_price NUMERIC(12,2) NOT NULL,
  created_at TIMESTAMP NOT NULL,
  FOREIGN KEY (order_id) REFERENCES orders(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE TABLE payments (
  id SERIAL PRIMARY KEY,
  order_id INTEGER NOT NULL,
  payment_method TEXT NOT NULL CHECK(payment_method IN ('credit_card','debit_card','paypal','apple_pay','google_pay')),
  amount NUMERIC(12,2) NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending','completed','failed','refunded')),
  transaction_id TEXT NOT NULL UNIQUE,
  paid_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE TABLE shipping (
  id SERIAL PRIMARY KEY,
  order_id INTEGER NOT NULL,
  carrier TEXT NOT NULL CHECK(carrier IN ('UPS','FedEx','USPS','DHL')),
  tracking_number TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL CHECK(status IN ('pending','picked_up','in_transit','out_for_delivery','delivered','returned')),
  shipped_at TIMESTAMP,
  estimated_delivery DATE,
  delivered_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE TABLE reviews (
  id SERIAL PRIMARY KEY,
  product_id INTEGER NOT NULL,
  customer_id INTEGER NOT NULL,
  order_id INTEGER NOT NULL,
  rating INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
  title TEXT,
  comment TEXT,
  is_verified BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMP NOT NULL,
  FOREIGN KEY (product_id) REFERENCES products(id),
  FOREIGN KEY (customer_id) REFERENCES customers(id),
  FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE TABLE feedback (
  id SERIAL PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  order_id INTEGER,
  type TEXT NOT NULL CHECK(type IN ('complaint','suggestion','praise','question')),
  subject TEXT NOT NULL,
  message TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('open','in_progress','resolved','closed')),
  priority TEXT NOT NULL CHECK(priority IN ('low','medium','high')),
  created_at TIMESTAMP NOT NULL,
  resolved_at TIMESTAMP,
  FOREIGN KEY (customer_id) REFERENCES customers(id),
  FOREIGN KEY (order_id) REFERENCES orders(id)
);