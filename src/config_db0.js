import "dotenv/config";

module.exports = {
  database: process.env.DB0,
  username: process.env.DB_USERNAME0,
  password: process.env.DB_PASS0,
  host: process.env.DB_HOST0,
  port: process.env.DB_PORT0,
  dialect: process.env.DB_DIALECT0,
};
