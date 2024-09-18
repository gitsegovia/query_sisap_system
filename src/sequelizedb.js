import { Sequelize } from "sequelize";
import config_db1 from "./config_db1";
import config_db2 from "./config_db2";
import config_db3 from "./config_db3";
import config_db4 from "./config_db4";

let sequelizeDB1 = new Sequelize(config_db1);
sequelizeDB1
  .authenticate()
  .then(() => console.log(`Conexión establecida a ${config_db1.host} ${config_db1.database}`))
  .catch((e) => console.log(e, `Error de Conexión a ${config_db1.host} ${config_db1.database}`));

let sequelizeDB2 = new Sequelize(config_db2);
sequelizeDB2
  .authenticate()
  .then(() => console.log(`Conexión establecida a ${config_db2.host} ${config_db2.database}`))
  .catch(() => console.log(`Error de Conexión a ${config_db2.host} ${config_db2.database}`));

let sequelizeDB3 = new Sequelize(config_db3);
sequelizeDB3
  .authenticate()
  .then(() => console.log(`Conexión establecida a ${config_db3.host} ${config_db3.database}`))
  .catch(() => console.log(`Error de Conexión a ${config_db3.host} ${config_db3.database}`));

let sequelizeDB4 = new Sequelize(config_db4);
sequelizeDB4
  .authenticate()
  .then(() => console.log(`Conexión establecida a ${config_db4.host} ${config_db4.database}`))
  .catch(() => console.log(`Error de Conexión a ${config_db4.host} ${config_db4.database}`));

const CONDITION_DB1 = "and cod_dep not in (1006,1021, 1027, 1028, 1035, 1036, 1037, 1038, 1039, 1040, 1041, 1042, 1043, 1045, 1046)";
const CONDITION_DB2 = "and cod_dep in (1006, 1021, 1027, 1028, 1036, 1037, 1038, 1039, 1040, 1042, 1043, 1045, 1046, 1047)";

const CONDITION_DB3 = "and cod_dep in (1035)";
const CONDITION_DB4 = "and cod_dep in (1041)";

async function unifiedQuery({ sqlQuery, table = "" }) {
  try {
    const [result1] = await sequelizeDB1.query(sqlQuery.replaceAll("[condition_ext]", CONDITION_DB1.replaceAll("cod_dep", `${table}cod_dep`)));
    const [result2] = await sequelizeDB2.query(sqlQuery.replaceAll("[condition_ext]", CONDITION_DB2.replaceAll("cod_dep", `${table}cod_dep`)));
    const [result3] = await sequelizeDB3.query(sqlQuery.replaceAll("[condition_ext]", CONDITION_DB3.replaceAll("cod_dep", `${table}cod_dep`)));
    const [result4] = await sequelizeDB4.query(sqlQuery.replaceAll("[condition_ext]", CONDITION_DB4.replaceAll("cod_dep", `${table}cod_dep`)));

    return [...result1, ...result2, ...result3, ...result4];
  } catch (error) {
    throw new Error(error);
  }
}

export async function identifiedQuery({ sqlQuery, table = "" }) {
  try {
    const [result_db1] = await sequelizeDB1.query(sqlQuery.replace("[condition_ext]", CONDITION_DB1.replace("cod_dep", `${table}cod_dep`)));
    const [result_db2] = await sequelizeDB2.query(sqlQuery.replace("[condition_ext]", CONDITION_DB2.replace("cod_dep", `${table}cod_dep`)));
    const [result_db3] = await sequelizeDB3.query(sqlQuery.replace("[condition_ext]", CONDITION_DB3.replace("cod_dep", `${table}cod_dep`)));
    const [result_db4] = await sequelizeDB4.query(sqlQuery.replace("[condition_ext]", CONDITION_DB4.replace("cod_dep", `${table}cod_dep`)));

    const result = {
      result_db1,
      result_db2,
      result_db3,
      result_db4,
    };
    console.log("resultado", result);
    return result;
  } catch (error) {
    throw new Error(error);
  }
}

export async function specificQuery({ sqlQuery, table = "", db = 1 }) {
  try {
    let result_db = [];
    switch (db) {
      case 2:
        [result_db] = await sequelizeDB2.query(sqlQuery.replace("[condition_ext]", CONDITION_DB2.replace("cod_dep", `${table}cod_dep`)));
        break;
      case 3:
        [result_db] = await sequelizeDB3.query(sqlQuery.replace("[condition_ext]", CONDITION_DB3.replace("cod_dep", `${table}cod_dep`)));
        break;

      case 4:
        [result_db] = await sequelizeDB4.query(sqlQuery.replace("[condition_ext]", CONDITION_DB4.replace("cod_dep", `${table}cod_dep`)));
        break;
      default:
        [result_db] = await sequelizeDB1.query(sqlQuery.replace("[condition_ext]", CONDITION_DB1.replace("cod_dep", `${table}cod_dep`)));
        break;
    }

    return result_db;
  } catch (error) {
    throw new Error(error);
  }
}

export default unifiedQuery;
