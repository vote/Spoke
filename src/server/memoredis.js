import createMemoizer from "memoredis";
import { config } from "../config";
import logger from "../logger";

const opts = config.MEMOREDIS_URL
  ? {
      clientOpts: config.MEMOREDIS_URL,
      prefix: config.MEMOREDIS_PREFIX,
      logger
    }
  : { emptyMode: true, logger };

const memoizer = createMemoizer(opts);

const ONE_SECOND = 1000;
const THIRTY_SECONDS = ONE_SECOND * 30;
const ONE_MINUTE = ONE_SECOND * 60;
const ONE_HOUR = ONE_MINUTE * 60;
const ONE_DAY = ONE_HOUR * 24;
const ONE_WEEK = ONE_DAY * 7;

const cacheOpts = {
  GetUser: ["get-user", ONE_HOUR],
  GetUserOrganization: ["get-user-organization", ONE_HOUR],
  CampaignHasUnassignedContacts: [
    "campaign-has-unassigned-contacts",
    THIRTY_SECONDS
  ],
  CampaignHasUnsentInitialMessages: [
    "campaign-has-unsent-initial-messages",
    THIRTY_SECONDS
  ],
  CampaignHasUnhandledMessages: [
    "campaign-has-unhandled-messages",
    THIRTY_SECONDS
  ],
  CampaignSentMessagesCount: ["campaign-sent-messages-count", ONE_MINUTE],
  CampaignReceivedMessagesCount: [
    "campaign-received-messages-count",
    ONE_MINUTE
  ],
  CampaignOptOutsCount: ["campaign-opt-outs-count", ONE_MINUTE],
  CampaignTeams: ["campaign-teams", ONE_WEEK],
  CampaignsList: ["campaigns-list", ONE_WEEK],
  CampaignOne: ["campaign-one", ONE_WEEK],
  CampaignInteractionSteps: ["campaign-interaction-steps", ONE_WEEK],
  CampaignCannedResponses: ["campaign-canned-responses", ONE_WEEK],
  InteractionStepChildren: ["interaction-step-children", ONE_WEEK],
  InteractionStepSingleton: ["interaction-step-singleton", ONE_WEEK],
  OrganizationTagList: ["organization-tag-list", ONE_WEEK],
  OrganizationEscalatedTagList: ["organization-escalated-tag-list", ONE_WEEK],
  OrganizationSingleTon: ["organization-singleton", ONE_HOUR],
  UserOrganizations: ["user-organizations", ONE_WEEK],
  UserOrganizationRoles: ["user-organization-roles", ONE_WEEK],
  CampaignOrganizationId: ["campaign-organiation-id", ONE_WEEK],
  FullfillAssignmentLock: ["fulfull-assignment-lock", ONE_MINUTE * 2]
};

Object.keys(cacheOpts).forEach(name => {
  cacheOpts[name] = {
    key: cacheOpts[name][0],
    ttl: cacheOpts[name][1]
  };
});

export { memoizer, cacheOpts };
