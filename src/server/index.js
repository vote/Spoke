import cron from "node-cron";
import express from "express";
import bodyParser from "body-parser";
import cookieSession from "cookie-session";
import basicAuth from "express-basic-auth";
import passport from "@passport-next/passport";
// import connectDatadog from "connect-datadog-graphql";
import { config } from "../config";
import logger from "../logger";
import appRenderer from "./middleware/app-renderer";
import { setupUserNotificationObservers } from "./notifications";
import { fulfillPendingRequestFor } from "./api/assignment";
import requestLogging from "../lib/request-logging";
import { checkForBadDeliverability } from "./api/lib/alerts";
import statsd from "./statsd";
import {
  authRouter,
  graphqlRouter,
  nexmoRouter,
  twilioRouter,
  assembleRouter,
  utilsRouter,
  previewRouter
} from "./routes";
import { getWorker } from "./worker";
import { errToObj } from "./utils";

// process.on("uncaughtException", ex => {
//   logger.error("uncaughtException: ", ex);
//   process.exit(1);
// });

process.on("unhandledRejection", err => {
  logger.error("unhandledRejection: ", err);
  process.exit(1);
});

cron.schedule("0 */1 * * *", checkForBadDeliverability);

setupUserNotificationObservers();

const {
  DEV_APP_PORT,
  PORT,
  PUBLIC_DIR,
  SESSION_SECRET,
  ASSIGNMENT_USERNAME,
  ASSIGNMENT_PASSWORD
} = config;

const app = express();

if (config.LOG_LEVEL === "verbose" || config.LOG_LEVEL === "debug") {
  app.use(requestLogging);
}

// Send version to client
if (config.SPOKE_VERSION) {
  app.use((_req, res, next) => {
    res.setHeader("x-spoke-version", config.SPOKE_VERSION);
    next();
  });
}

app.enable("trust proxy"); // Don't rate limit heroku
app.use(bodyParser.json({ limit: "50mb" }));
app.use(bodyParser.urlencoded({ extended: true }));
app.use(
  cookieSession({
    cookie: {
      httpOnly: true,
      secure: config.isProduction,
      maxAge: null
    },
    secret: SESSION_SECRET
  })
);
app.use(passport.initialize());
app.use(passport.session());

if (PUBLIC_DIR) {
  app.use(express.static(PUBLIC_DIR, { maxAge: "180 days" }));
}

if (statsd.isEnabled) {
  const datadogOptions = {
    dogstatsd: statsd.getClient(),
    path: true,
    method: false,
    response_code: true,
    graphql_paths: ["/graphql"]
  };

  if (config.CLIENT_NAME) {
    datadogOptions.tags.push(`client:${config.CLIENT_NAME}`);
  }

  // app.use(connectDatadog(datadogOptions));
}

app.use(authRouter);
app.use(graphqlRouter);
app.use(nexmoRouter);
app.use(twilioRouter);
app.use(assembleRouter);
app.use(utilsRouter);
app.use(previewRouter);

app.post(
  "/autoassign",
  basicAuth({
    users: {
      [ASSIGNMENT_USERNAME]: ASSIGNMENT_PASSWORD
    }
  }),
  async (req, res) => {
    if (!req.body.slack_id)
      return res
        .status(400)
        .json({ error: "Missing parameter `slack_id` in POST body." });
    if (!req.body.count)
      return res
        .status(400)
        .json({ error: "Missing parameter `count` in POST body." });

    try {
      const numberAssigned = await fulfillPendingRequestFor(req.body.slack_id);
      return res.json({ numberAssigned });
    } catch (err) {
      logger.error("Error handling autoassignment request: ", err);
      return err.isFatal
        ? res.status(500).json({ error: err.message })
        : res.status(200).json({
            numberAssigned: 0,
            info: err.message
          });
    }
  }
);

// This middleware should be last. Return the React app only if no other route is hit.
app.use(appRenderer);

// Custom error handling
app.use((err, req, res, next) => {
  logger.warn("Unhandled express error: ", {
    error: errToObj(err),
    req
  });
  if (res.headersSent) {
    return next(err);
  }
  return res.status(500).json({ error: true });
});

const port = DEV_APP_PORT || PORT;
app.listen(port, () => {
  logger.info(`Node app is running on port ${port}`);
});

getWorker();

// Used by lambda handler
export default app;
