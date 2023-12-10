const cors = require("cors");
const express = require("express");
const app = express();

// let corsOptions = {
//   origin: "http://localhost:80",
// };
// app.use(cors());
// app.use(cors(corsOptions));
// 


app.use(cors({ origin: '*' }));

const initRoutes = require("./src/routes");

app.use(express.urlencoded({ extended: true }));
initRoutes(app);

const port = 5000;
app.listen(port, () => {
  console.log(`Running at localhost:${port}`);
});