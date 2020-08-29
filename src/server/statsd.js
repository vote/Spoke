import hotShots from "hot-shots";
import { config } from "../config";

const isEnabled = !!(config.DD_AGENT_HOST && config.DD_DOGSTATSD_PORT);

let client;

const getClient = () => {
  if (isEnabled && !client) {
    client = new hotShots.StatsD({
      host: config.DD_AGENT_HOST,
      port: config.DD_DOGSTATSD_PORT,
      globalTags: config.DD_TAGS.split(",")
    });
  }
  return client;
};

export default {
  isEnabled,
  getClient,
};
