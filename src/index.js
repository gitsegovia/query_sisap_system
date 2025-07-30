import "dotenv/config";
import express from "express";
import path from "path";
import routes from "./routes";
import { engine } from "express-handlebars";
import cors from "cors";

const PORT = process.env.PORT || 3000;

//Server
const app = express();

//Engine
app.engine("handlebars", engine());
app.set("view engine", "handlebars");
app.set("views", path.join(__dirname, "/views"));

app.use(cors("*"));
//Routers
app.use(routes);

//404 Route
app.use(function (req, res, next) {
  res.status(404).render("404.", { layout: false });
});

app.listen(PORT, () => {
  console.log(`✔✔✔ Server Query SISAP System is running on port ${PORT} -- 'http://127.0.0.1:${PORT}'`);
});
