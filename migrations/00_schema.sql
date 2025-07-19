create schema core;

comment on schema core is $$
  The core schema contains all business logic functions and views organized by domain.
  
  This is the service layer that sits between the public tables and the API schema.
  It encapsulates business rules, computed views, and helper functions.
  
  Organization:
  - 01_account: Account and seller related logic
  - 02_product: Product and listing related logic  
  - 03_checkout: Checkout and payment processing logic
  - 04_order: Order fulfillment, shipping, and refunds logic
  - 05_common: Shared utilities and helper functions
$$;

create schema api;

create schema jobs;