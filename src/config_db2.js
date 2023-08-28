import 'dotenv/config';

module.exports = {
  database: process.env.DB2,
  username: process.env.DB_USERNAME2,
  password: process.env.DB_PASS2,
  host: process.env.DB_HOST2, 
  port: process.env.DB_PORT2, 
  dialect: process.env.DB_DIALECT2
}