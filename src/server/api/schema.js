import { config } from "../../config";
import logger from "../../logger";
import { errToObj } from "../utils";
import { eventBus, EventType } from "../event-bus";
import escapeRegExp from "lodash/escapeRegExp";
import camelCaseKeys from "camelcase-keys";
import GraphQLDate from "graphql-date";
import GraphQLJSON from "graphql-type-json";
import { GraphQLError } from "graphql/error";
import request from "superagent";
import _ from "lodash";
import moment from "moment-timezone";

import { CampaignExportType } from "../../api/types";
import { getWorker } from "../worker";
import { processContactsFile } from "./lib/edit-campaign";
import { formatPage } from "./lib/pagination";
import { TextRequestType } from "../../api/organization";
import { gzip, makeTree } from "../../lib";
import { applyScript } from "../../lib/scripts";
import { hasRole } from "../../lib/permissions";
import {
  assignTexters,
  exportCampaign,
  loadContactsFromDataWarehouse,
  uploadContacts,
  filterLandlines
} from "../../workers/jobs";
import { exportForVan } from "../../workers/jobs/export-for-van";
import { datawarehouse, r, cacheableData } from "../models";
import { Notifications, sendUserNotification } from "../notifications";
import {
  resolvers as assignmentResolvers,
  giveUserMoreTexts,
  myCurrentAssignmentTarget
} from "./assignment";
import { getCampaigns, resolvers as campaignResolvers } from "./campaign";
import { resolvers as campaignContactResolvers } from "./campaign-contact";
import { resolvers as cannedResponseResolvers } from "./canned-response";
import {
  getConversations,
  getCampaignIdMessageIdsAndCampaignIdContactIdsMaps,
  getCampaignIdMessageIdsAndCampaignIdContactIdsMapsChunked,
  reassignContacts,
  reassignConversations,
  resolvers as conversationsResolver
} from "./conversations";
import {
  accessRequired,
  assignmentRequired,
  authRequired,
  superAdminRequired,
  assignmentRequiredOrHasOrgRoleForCampaign
} from "./errors";
import { resolvers as interactionStepResolvers } from "./interaction-step";
import { resolvers as inviteResolvers } from "./invite";
import { resolvers as linkDomainResolvers } from "./link-domain";
import {
  saveNewIncomingMessage,
  getContactMessagingService
} from "./lib/message-sending";
import { graphileSecretRef } from "./lib/utils";
import serviceMap from "./lib/services";
import { resolvers as messageResolvers } from "./message";
import { resolvers as optOutResolvers } from "./opt-out";
import {
  resolvers as organizationResolvers,
  getEscalationUserId
} from "./organization";
import { resolvers as membershipSchema } from "./organization-membership";
import { RequestAutoApproveType } from "../../api/organization-membership";
import {
  resolvers as settingsSchema,
  updateOrganizationSettings
} from "./organization-settings";
import { GraphQLPhone } from "./phone";
import { resolvers as questionResolvers } from "./question";
import { resolvers as questionResponseResolvers } from "./question-response";
import { getUsers, getUsersById, resolvers as userResolvers } from "./user";
import { resolvers as assignmentRequestResolvers } from "./assignment-request";
import { resolvers as tagResolvers } from "./tag";
import { resolvers as teamResolvers } from "./team";
import { resolvers as trollbotResolvers } from "./trollbot";
import { resolvers as externalListResolvers } from "./external-list";
import { resolvers as externalSystemResolvers } from "./external-system";
import {
  queryCampaignOverlaps,
  queryCampaignOverlapCount
} from "./campaign-overlap";
import { change } from "../local-auth-helpers";
import { notifyOnTagConversation, notifyAssignmentCreated } from "./lib/alerts";

import { isNowBetween } from "../../lib/timezones";
import { memoizer, cacheOpts } from "../memoredis";
import groupBy from "lodash/groupBy";

const uuidv4 = require("uuid").v4;
const JOBS_SAME_PROCESS = config.JOBS_SAME_PROCESS;
const JOBS_SYNC = config.JOBS_SYNC;

const replaceCurlyApostrophes = rawText =>
  rawText.replace(/[\u2018\u2019]/g, "'");

const replaceAll = (str, find, replace) =>
  str.replace(new RegExp(escapeRegExp(find), "g"), replace);

const replaceShortLinkDomains = async (organizationId, messageText) => {
  const domains = await r
    .knex("link_domain")
    .where({ organization_id: organizationId })
    .pluck("domain");

  const checkerReducer = (doesContainShortlink, linkDomain) => {
    const containsLinkDomain = messageText.indexOf(linkDomain) > -1;
    return doesContainShortlink || containsLinkDomain;
  };
  const doesContainShortLink = domains.reduce(checkerReducer, false);

  if (!doesContainShortLink) {
    return messageText;
  }

  // Get next domain
  const domainRaw = await r.knex.raw(
    `
    update
      link_domain
    set
      current_usage_count = (current_usage_count + 1) % max_usage_count,
      cycled_out_at = case when (current_usage_count + 1) % max_usage_count = 0 then now() else cycled_out_at end
    where
      id = (
        select
          id
        from
          link_domain
        where
          is_manually_disabled = false
          and organization_id = ?
        and not exists (
          select 1
          from unhealthy_link_domain
          where unhealthy_link_domain.domain = link_domain.domain
        )
        order by
          cycled_out_at asc,
          current_usage_count asc
        limit 1
        for update
        skip locked
      )
    returning link_domain.domain;
  `,
    [organizationId]
  );
  const targetDomain = domainRaw.rows[0] && domainRaw.rows[0].domain;

  // Skip updating the message text if no healthy target domain was found
  if (!targetDomain) {
    return messageText;
  }

  const replacerReducer = (text, domain) => {
    const safeDomain = escapeRegExp(domain);
    const domainRegex = RegExp(`(https?://)${safeDomain}(:*)`, "g");
    return text.replace(domainRegex, "$1" + targetDomain + "$2");
  };
  const finalMessageText = domains.reduce(replacerReducer, messageText);
  return finalMessageText;
};

async function editCampaign(id, campaign, loaders, user, origCampaignRecord) {
  const {
    title,
    description,
    dueBy,
    useDynamicAssignment,
    logoImageUrl,
    introHtml,
    primaryColor,
    textingHoursStart,
    textingHoursEnd,
    isAutoassignEnabled,
    repliesStaleAfter,
    timezone
  } = campaign;

  const organizationId = origCampaignRecord.organization_id;
  const campaignUpdates = {
    id,
    title,
    description,
    due_by: dueBy,
    organization_id: organizationId,
    // TODO: re-enable once dynamic assignment is fixed (#548)
    // use_dynamic_assignment: useDynamicAssignment,
    logo_image_url: logoImageUrl,
    primary_color: primaryColor,
    intro_html: introHtml,
    texting_hours_start: textingHoursStart,
    texting_hours_end: textingHoursEnd,
    is_autoassign_enabled: isAutoassignEnabled,
    replies_stale_after_minutes: repliesStaleAfter, // this is null to unset it - it must be null, not undefined
    timezone
  };

  Object.keys(campaignUpdates).forEach(key => {
    if (typeof campaignUpdates[key] === "undefined") {
      delete campaignUpdates[key];
    }
  });

  if (campaign.hasOwnProperty("externalListId") && campaign.externalListId) {
    await r
      .knex("campaign_contact")
      .where({ campaign_id: id })
      .del();
    await r.knex.raw(
      `select * from public.queue_load_list_into_campaign(?, ?)`,
      [id, parseInt(campaign.externalListId)]
    );
  }

  let validationStats = {};
  if (campaign.hasOwnProperty("contactsFile") && campaign.contactsFile) {
    const processedContacts = await processContactsFile(campaign.contactsFile);
    campaign.contacts = processedContacts.contacts;
    validationStats = processedContacts.validationStats;
  }

  if (campaign.hasOwnProperty("contacts") && campaign.contacts) {
    await accessRequired(user, organizationId, "ADMIN", /* superadmin*/ true);
    const contactsToSave = campaign.contacts.map(datum => {
      const modelData = {
        campaign_id: datum.campaignId,
        first_name: datum.firstName,
        last_name: datum.lastName,
        cell: datum.cell,
        external_id: datum.external_id,
        custom_fields: datum.customFields,
        message_status: "needsMessage",
        is_opted_out: false,
        zip: datum.zip || ""
      };
      modelData.campaign_id = id;
      return modelData;
    });
    const jobPayload = {
      excludeCampaignIds: campaign.excludeCampaignIds || [],
      contacts: contactsToSave,
      filterOutLandlines: campaign.filterOutLandlines,
      validationStats
    };
    const compressedString = await gzip(JSON.stringify(jobPayload));
    const [job] = await r
      .knex("job_request")
      .insert({
        queue_name: `${id}:edit_campaign`,
        job_type: "upload_contacts",
        locks_queue: true,
        assigned: JOBS_SAME_PROCESS, // can get called immediately, below
        campaign_id: id,
        // NOTE: stringifying because compressedString is a binary buffer
        payload: compressedString.toString("base64")
      })
      .returning("*");
    if (JOBS_SAME_PROCESS) {
      uploadContacts(job);
    }
  }
  if (
    campaign.hasOwnProperty("contactSql") &&
    datawarehouse &&
    user.is_superadmin
  ) {
    await accessRequired(user, organizationId, "ADMIN", /* superadmin*/ true);
    const [job] = await r
      .knex("job_request")
      .insert({
        queue_name: `${id}:edit_campaign`,
        job_type: "upload_contacts_sql",
        locks_queue: true,
        assigned: JOBS_SAME_PROCESS, // can get called immediately, below
        campaign_id: id,
        payload: campaign.contactSql
      })
      .returning("*");
    if (JOBS_SAME_PROCESS) {
      loadContactsFromDataWarehouse(job);
    }
  }
  if (campaign.hasOwnProperty("isAssignmentLimitedToTeams")) {
    await r
      .knex("campaign")
      .update({
        limit_assignment_to_teams: campaign.isAssignmentLimitedToTeams
      })
      .where({ id });
  }
  if (campaign.hasOwnProperty("teamIds")) {
    await r.knex.transaction(async trx => {
      // Remove all existing team memberships and then add everything again
      await trx("campaign_team")
        .where({ campaign_id: id })
        .del();
      await trx("campaign_team").insert(
        campaign.teamIds.map(team_id => ({ team_id, campaign_id: id }))
      );
    });
    memoizer.invalidate(cacheOpts.CampaignTeams.key, { campaignId: id });
  }
  if (campaign.hasOwnProperty("texters")) {
    const [job] = await r
      .knex("job_request")
      .insert({
        queue_name: `${id}:edit_campaign`,
        locks_queue: true,
        assigned: JOBS_SAME_PROCESS, // can get called immediately, below
        job_type: "assign_texters",
        campaign_id: id,
        payload: JSON.stringify({
          id,
          texters: campaign.texters
        })
      })
      .returning("*");

    if (JOBS_SAME_PROCESS) {
      if (JOBS_SYNC) {
        await assignTexters(job);
      } else {
        assignTexters(job);
      }
    }
  }

  if (campaign.hasOwnProperty("interactionSteps")) {
    memoizer.invalidate(cacheOpts.CampaignInteractionSteps.key, {
      campaignId: id
    });
    // TODO: debug why { script: '' } is even being sent from the client in the first place
    if (!_.isEqual(campaign.interactionSteps, { scriptOptions: [""] })) {
      await accessRequired(
        user,
        organizationId,
        "SUPERVOLUNTEER",
        /* superadmin*/ true
      );
      await persistInteractionStepTree(
        id,
        campaign.interactionSteps,
        origCampaignRecord
      );
    }
  }

  if (campaign.hasOwnProperty("cannedResponses")) {
    memoizer.invalidate(cacheOpts.CampaignCannedResponses.key, {
      campaignId: id
    });

    // Ignore the mocked `id` automatically created on the input by GraphQL
    const convertedResponses = campaign.cannedResponses.map(
      ({ id: _cannedResponseId, ...response }) => ({
        ...response,
        campaign_id: id
      })
    );

    await r
      .knex("canned_response")
      .where({ campaign_id: id })
      .whereNull("user_id")
      .del();
    await r.knex("canned_response").insert(convertedResponses);
    await cacheableData.cannedResponse.clearQuery({
      userId: "",
      campaignId: id
    });
  }

  const [newCampaign] = await r
    .knex("campaign")
    .update(campaignUpdates)
    .where({ id })
    .returning("*");
  cacheableData.campaign.reload(id);
  return newCampaign || loaders.campaign.load(id);
}

const persistInteractionStepTree = async (
  campaignId,
  rootInteractionStep,
  origCampaignRecord,
  knexTrx,
  temporaryIdMap = {}
) => {
  // Perform updates in a transaction if one is not present
  if (!knexTrx) {
    return await r.knex.transaction(async trx => {
      await persistInteractionStepTree(
        campaignId,
        rootInteractionStep,
        origCampaignRecord,
        trx,
        temporaryIdMap
      );
    });
  }

  // Update the parent interaction step ID if this step has a reference to a temporary ID
  // and the parent has since been inserted
  if (temporaryIdMap[rootInteractionStep.parentInteractionId]) {
    rootInteractionStep.parentInteractionId =
      temporaryIdMap[rootInteractionStep.parentInteractionId];
  }

  if (rootInteractionStep.id.indexOf("new") !== -1) {
    // Insert new interaction steps
    const [newId] = await knexTrx("interaction_step")
      .insert({
        parent_interaction_id: rootInteractionStep.parentInteractionId || null,
        question: rootInteractionStep.questionText,
        script_options: rootInteractionStep.scriptOptions,
        answer_option: rootInteractionStep.answerOption,
        answer_actions: rootInteractionStep.answerActions,
        campaign_id: campaignId,
        is_deleted: false
      })
      .returning("id");

    if (rootInteractionStep.parentInteractionId) {
      memoizer.invalidate(cacheOpts.InteractionStepChildren.key, {
        interactionStepId: rootInteractionStep.parentInteractionId
      });
    }

    // Update the mapping of temporary IDs
    temporaryIdMap[rootInteractionStep.id] = newId;
  } else if (!origCampaignRecord.is_started && rootInteractionStep.isDeleted) {
    // Hard delete interaction steps if the campaign hasn't started
    await knexTrx("interaction_step")
      .where({ id: rootInteractionStep.id })
      .delete();
  } else {
    // Update the interaction step record
    await knexTrx("interaction_step")
      .where({ id: rootInteractionStep.id })
      .update({
        question: rootInteractionStep.questionText,
        script_options: rootInteractionStep.scriptOptions,
        answer_option: rootInteractionStep.answerOption,
        answer_actions: rootInteractionStep.answerActions,
        is_deleted: rootInteractionStep.isDeleted
      });

    memoizer.invalidate(cacheOpts.InteractionStepSingleton.key, {
      interactionStepId: rootInteractionStep.id
    });
  }

  // Persist child interaction steps
  await Promise.all(
    rootInteractionStep.interactionSteps.map(async childStep => {
      await persistInteractionStepTree(
        campaignId,
        childStep,
        origCampaignRecord,
        knexTrx,
        temporaryIdMap
      );
    })
  );
};

// We've modified campaign creation on the client so that overrideOrganizationHours is always true
// and enforce_texting_hours is always true
// as a result, we're forcing admins to think about the time zone of each campaign
// and saving a join on this query.
async function sendMessage(
  user,
  campaignContactId,
  message,
  checkOptOut = true,
  checkAssignment = true,
  skipUpdatingMessageStatus = false
) {
  // Scope opt-outs to organization if we are not sharing across all organizations
  const optOutCondition = !config.OPTOUTS_SHARE_ALL_ORGS
    ? "and opt_out.organization_id = campaign.organization_id"
    : "";

  const record = await r
    .knex("campaign_contact")
    .join("campaign", "campaign_contact.campaign_id", "campaign.id")
    .where({ "campaign_contact.id": parseInt(campaignContactId) })
    .whereRaw("campaign_contact.archived = false")
    .where({ "campaign.is_archived": false })
    .leftJoin("assignment", "campaign_contact.assignment_id", "assignment.id")
    .first(
      "campaign_contact.id as cc_id",
      "campaign_contact.assignment_id as assignment_id",
      "campaign_contact.message_status as cc_message_status",
      "campaign.id as campaign_id",
      "campaign.is_archived as is_archived",
      "campaign.organization_id as organization_id",
      "campaign.timezone as c_timezone",
      "campaign.texting_hours_start as c_texting_hours_start",
      "campaign.texting_hours_end as c_texting_hours_end",
      "assignment.user_id as a_assignment_user_id",
      r.knex.raw(
        `exists (
          select 1
          from opt_out
          where
            opt_out.cell = campaign_contact.cell
              ${optOutCondition}
        ) as is_opted_out`
      ),
      "campaign_contact.timezone as contact_timezone"
    );

  // If the conversation is unassigned, create an assignment. This assignment will be applied to
  // the message only, and not the campaign contact. We don't use message.assignment_id and the
  // cleaner solution would be to remove the column entirely. I object to this workaround!!
  // - @bchrobot
  const isConversationUnassigned =
    record.assignment_id === null && message.assignmentId === null;
  if (isConversationUnassigned) {
    // Check for existing assignment
    const assignment = await r
      .knex("assignment")
      .where({
        user_id: user.id,
        campaign_id: record.campaign_id
      })
      .first("id");
    if (assignment && assignment.id) {
      record.assignment_id = assignment.id;
    } else {
      // Create assignment if no exisiting
      const [newAssignment] = await r
        .knex("assignment")
        .insert({
          user_id: user.id,
          campaign_id: record.campaign_id
        })
        .returning("*");
      eventBus.emit(EventType.AssignmentCreated, newAssignment);
      record.assignment_id = newAssignment.id;
    }
    message.assignmentId = record.assignment_id;
  }

  const assignmentIdsMatch =
    record.assignment_id === parseInt(message.assignmentId);
  if (checkAssignment && !assignmentIdsMatch) {
    throw new GraphQLError("Your assignment has changed");
  }

  // setting defaults based on new forced conditions
  record.o_texting_hours_enforced = true;
  record.o_texting_hours_end = 21;

  // This block will only need to be evaluated if message is sent from admin Message Review
  if (record.a_assignment_user_id !== user.id) {
    const currentRoles = await r
      .knex("user_organization")
      .where({
        user_id: user.id,
        organization_id: record.organization_id
      })
      .pluck("role");
    const isAdmin = hasRole("SUPERVOLUNTEER", currentRoles);
    if (!isAdmin) {
      throw new GraphQLError(
        "You are not authorized to send a message for this assignment!"
      );
    }
  }

  if (checkOptOut && !!record.is_opted_out) {
    throw new GraphQLError(
      "Skipped sending because this contact was already opted out"
    );
  }

  const {
    contact_timezone: contactTimezone,
    c_timezone: campaignTimezone,
    c_texting_hours_start: startHour,
    c_texting_hours_end: endHour
  } = record;
  const timezone = contactTimezone || campaignTimezone;
  const isValidSendTime = isNowBetween(timezone, startHour, endHour);

  if (!isValidSendTime) {
    throw new GraphQLError("Outside permitted texting time for this recipient");
  }

  const sendBefore = moment()
    .tz(timezone)
    .startOf("day")
    .hour(endHour)
    .utc();

  const { contactNumber, text } = message;

  if (text.length > (config.MAX_MESSAGE_LENGTH || 99999)) {
    throw new GraphQLError("Message was longer than the limit");
  }

  const escapedApostrophes = replaceCurlyApostrophes(text);
  const replacedDomainsText = await replaceShortLinkDomains(
    record.organization_id,
    escapedApostrophes
  );

  const { service_type } = await getContactMessagingService(
    campaignContactId,
    record.organization_id
  );

  const toInsert = {
    user_id: user.id,
    campaign_contact_id: campaignContactId,
    text: replacedDomainsText,
    contact_number: contactNumber,
    user_number: "",
    assignment_id: message.assignmentId,
    send_status: JOBS_SAME_PROCESS ? "SENDING" : "QUEUED",
    service: service_type,
    is_from_contact: false,
    queued_at: new Date(),
    send_before: sendBefore,
    script_version_hash: message.versionHash
  };

  const messageSavePromise = r
    .knex("message")
    .insert(toInsert)
    .returning(Object.keys(toInsert).concat(["id"]));

  const { cc_message_status } = record;
  const contactSavePromise = (async () => {
    if (!skipUpdatingMessageStatus) {
      await r
        .knex("campaign_contact")
        .update({
          message_status:
            cc_message_status === "needsResponse" ||
            cc_message_status === "convo"
              ? "convo"
              : "messaged"
        })
        .where({ id: record.cc_id });
    }

    const contact = await r
      .knex("campaign_contact")
      .select("*")
      .where({ id: record.cc_id })
      .first();
    return contact;
  })();

  const [messageInsertResult, contactUpdateResult] = await Promise.all([
    messageSavePromise,
    contactSavePromise
  ]);
  const messageInstance = Array.isArray(messageInsertResult)
    ? messageInsertResult[0]
    : messageInsertResult;
  toInsert.id = messageInstance.id || messageInstance;

  // Send message after we are sure messageInstance has been persisted
  const service = serviceMap[service_type];
  service.sendMessage(toInsert, record.organization_id);

  // Send message to BernieSMS to be checked for bad words
  const badWordUrl = config.BAD_WORD_URL;
  if (badWordUrl) {
    request
      .post(badWordUrl)
      .timeout(5000)
      .set("Authorization", `Token ${config.BAD_WORD_TOKEN}`)
      .send({ user_id: user.auth0_id, message: toInsert.text })
      .end((err, res) => {
        if (err) {
          logger.error("Error submitting message to bad word service: ", err);
        }
      });
  }

  return contactUpdateResult;
}

const rootMutations = {
  RootMutation: {
    userAgreeTerms: async (_, { userId }, { user, loaders }) => {
      // TODO: permissions check needed -- user.id === userId
      const [currentUser] = await r
        .knex("user")
        .where({ id: userId })
        .update({ terms: true })
        .returning("*");
      return currentUser;
    },

    sendReply: async (_, { id, message }, { user, loaders }) => {
      const contact = await loaders.campaignContact.load(id);
      const campaign = await loaders.campaign.load(contact.campaign_id);

      await accessRequired(user, campaign.organization_id, "ADMIN");

      const lastMessage = await r
        .knex("message")
        .where({
          assignment_id: contact.assignment_id,
          contact_number: contact.cell
        })
        .first();

      if (!lastMessage) {
        throw new GraphQLError(
          "Cannot fake a reply to a contact that has no existing thread yet"
        );
      }

      const userNumber = lastMessage.user_number;
      const contactNumber = contact.cell;
      const mockId = `mocked_${Math.random()
        .toString(36)
        .replace(/[^a-zA-Z1-9]+/g, "")}`;
      await saveNewIncomingMessage({
        campaign_contact_id: contact.id,
        contact_number: contactNumber,
        user_number: userNumber,
        is_from_contact: true,
        text: message,
        service_response: JSON.stringify([
          {
            fakeMessage: true,
            userId: user.id,
            userFirstName: user.first_name
          }
        ]),
        service_id: mockId,
        assignment_id: lastMessage.assignment_id,
        service: lastMessage.service,
        send_status: "DELIVERED"
      });
      return loaders.campaignContact.load(id);
    },

    exportCampaign: async (_, { options }, { user, loaders }) => {
      const { campaignId, exportType, vanOptions } = options;

      if (exportType === CampaignExportType.VAN && !vanOptions) {
        throw new Error("Input must include vanOptions when exposting as VAN!");
      }

      const campaign = await loaders.campaign.load(campaignId);
      const organizationId = campaign.organization_id;
      await accessRequired(user, organizationId, "ADMIN");

      const jobTypes = {};
      jobTypes[CampaignExportType.SPOKE] = "export";
      jobTypes[CampaignExportType.VAN] = "van-export";

      let payload = {};
      if (exportType === CampaignExportType.SPOKE) {
        payload = { id: campaignId, requester: user.id };
      } else if (exportType === CampaignExportType.VAN) {
        payload = { ...vanOptions, requesterId: user.id };
      }

      const [newJob] = await r
        .knex("job_request")
        .insert({
          queue_name: `${campaignId}:export`,
          job_type: jobTypes[exportType],
          locks_queue: false,
          assigned: JOBS_SAME_PROCESS, // can get called immediately, below
          campaign_id: campaignId,
          payload: JSON.stringify(payload)
        })
        .returning("*");
      if (JOBS_SAME_PROCESS) {
        if (exportType === CampaignExportType.SPOKE) {
          exportCampaign(newJob);
        } else if (exportType === CampaignExportType.VAN) {
          exportForVan(newJob);
        }
      }
      return newJob;
    },

    editOrganizationMembership: async (
      _,
      { id, level, role },
      { user: authUser }
    ) => {
      const membership = await r
        .knex("user_organization")
        .where({ id: parseInt(id) })
        .first();
      if (!membership) throw new Error("No such org membership");

      let roleRequired = "ADMIN";
      if (role && (membership.role === "OWNER" || role === "OWNER")) {
        roleRequired = "OWNER";
      }

      await accessRequired(
        authUser,
        membership.organization_id,
        roleRequired,
        true
      );

      const updateQuery = r
        .knex("user_organization")
        .where({
          user_id: membership.user_id,
          organization_id: membership.organization_id
        })
        .returning("*");

      if (level) updateQuery.update({ request_status: level.toLowerCase() });
      if (role) updateQuery.update({ role });

      const [orgMembership] = await updateQuery;

      memoizer.invalidate(cacheOpts.UserOrganizations.key, {
        userId: membership.user_id
      });
      memoizer.invalidate(cacheOpts.UserOrganizationRoles.key, {
        userId: membership.user_id,
        organizationId: membership.organization_id
      });

      return orgMembership;
    },

    editOrganizationSettings: async (_, { id, input }, { user: authUser }) => {
      const organizationId = parseInt(id);
      await accessRequired(authUser, organizationId, "OWNER");
      const updatedOrganization = await updateOrganizationSettings(
        organizationId,
        input
      );
      return updatedOrganization;
    },

    editUser: async (_, { organizationId, userId, userData }, { user }) => {
      if (user.id !== userId) {
        // User can edit themselves
        await accessRequired(user, organizationId, "ADMIN", true);
      }
      const userRes = await r
        .knex("user")
        .join("user_organization", "user.id", "user_organization.user_id")
        .where({
          "user_organization.organization_id": organizationId,
          "user.id": userId
        })
        .limit(1);
      if (!userRes || !userRes.length) {
        return null;
      } else {
        const member = userRes[0];
        if (userData) {
          const userRes = await r
            .knex("user")
            .where("id", userId)
            .update({
              first_name: userData.firstName,
              last_name: userData.lastName,
              email: userData.email,
              cell: userData.cell
            });

          memoizer.invalidate(cacheOpts.GetUser.key, { id: userId });
          memoizer.invalidate(cacheOpts.GetUser.key, {
            auth0Id: userRes.auth0_id
          });

          userData = {
            id: userId,
            first_name: userData.firstName,
            last_name: userData.lastName,
            email: userData.email,
            cell: userData.cell
          };
        } else {
          userData = member;
        }
        return userData;
      }
    },

    resetUserPassword: async (_, { organizationId, userId }, { user }) => {
      if (config.PASSPORT_STRATEGY !== "local")
        throw new Error(
          "Password reset may only be used with the 'local' login strategy."
        );
      if (user.id === userId) {
        throw new Error("You can't reset your own password.");
      }
      await accessRequired(user, organizationId, "ADMIN", true);

      // Add date at the end in case user record is modified after password is reset
      const passwordResetHash = uuidv4();
      const auth0_id = `reset|${passwordResetHash}|${Date.now()}`;

      const userRes = await r
        .knex("user")
        .where("id", userId)
        .update({
          auth0_id
        });
      return passwordResetHash;
    },

    changeUserPassword: async (_, { userId, formData }, { user }) => {
      if (user.id !== userId) {
        throw new Error("You can only change your own password.");
      }

      const { password, newPassword, passwordConfirm } = formData;

      const updatedUser = await change({
        user,
        password,
        newPassword,
        passwordConfirm
      });

      return updatedUser;
    },

    joinOrganization: async (_, { organizationUuid }, { user, loaders }) => {
      const organization = await r
        .knex("organization")
        .where("uuid", organizationUuid)
        .first();

      if (!organization) {
        logger.info("User tried to join non-existent organization", {
          organizationUuid,
          user
        });
        throw new Error("No such organization.");
      }

      const existingMembership = await r
        .knex("user_organization")
        .where({
          user_id: user.id,
          organization_id: organization.id
        })
        .first();

      if (existingMembership) {
        logger.info("User tried to join organization they're already part of", {
          organizationId: organization.id,
          userId: user.id
        });
        return organization;
      }

      let approvalStatus = RequestAutoApproveType.APPROVAL_REQUIRED;
      try {
        approvalStatus =
          JSON.parse(organization.features || "{}")
            .defaulTexterApprovalStatus || approvalStatus;
      } catch (err) {}

      await r.knex("user_organization").insert({
        user_id: user.id,
        organization_id: organization.id,
        role: "TEXTER",
        request_status: approvalStatus.toLowerCase()
      });

      return organization;
    },

    assignUserToCampaign: async (
      _,
      { organizationUuid, campaignId },
      { user, loaders }
    ) => {
      // TODO: re-enable once dynamic assignment is fixed (#548)
      throw new Error("Invalid join request");
      const campaign = await r
        .knex("campaign")
        .join("organization", "campaign.organization_id", "organization.id")
        .where({
          "campaign.id": parseInt(campaignId),
          "campaign.use_dynamic_assignment": true,
          "organization.uuid": organizationUuid
        })
        .select("campaign.*")
        .first();
      if (!campaign) {
        throw new Error("Invalid join request");
      }
      const assignment = await r
        .knex("assignment")
        .where({
          user_id: user.id,
          campaign_id: campaign.id
        })
        .first();
      if (!assignment) {
        const [newAssignment] = await r
          .knex("assignment")
          .insert({
            user_id: user.id,
            campaign_id: campaign.id,
            max_contacts: config.MAX_CONTACTS_PER_TEXTER
          })
          .returning("*");
        eventBus.emit(EventType.AssignmentCreated, newAssignment);
      }
      return campaign;
    },

    updateTextingHours: async (
      _,
      { organizationId, textingHoursStart, textingHoursEnd },
      { user }
    ) => {
      await accessRequired(user, organizationId, "OWNER");

      await r
        .knex("organization")
        .update({
          texting_hours_start: textingHoursStart,
          texting_hours_end: textingHoursEnd
        })
        .where({ id: organizationId });
      cacheableData.organization.clear(organizationId);

      return await r
        .knex("organization")
        .where({ id: organizationId })
        .first();
    },

    updateTextingHoursEnforcement: async (
      _,
      { organizationId, textingHoursEnforced },
      { user, loaders }
    ) => {
      await accessRequired(user, organizationId, "SUPERVOLUNTEER");

      await r
        .knex("organization")
        .update({
          texting_hours_enforced: textingHoursEnforced
        })
        .where({ id: organizationId });
      await cacheableData.organization.clear(organizationId);

      return await loaders.organization.load(organizationId);
    },

    updateTextRequestFormSettings: async (_, args, { user, loaders }) => {
      const {
        organizationId,
        textRequestFormEnabled,
        textRequestType,
        textRequestMaxCount
      } = args;
      await accessRequired(user, organizationId, "ADMIN");

      const currentOrganization = await r
        .knex("organization")
        .where({ id: organizationId })
        .first();
      let currentFeatures = {};
      try {
        currentFeatures = JSON.parse(currentOrganization.features);
      } catch (ex) {
        // do nothing
      }

      let nextFeatures = {
        textRequestFormEnabled,
        textRequestType,
        textRequestMaxCount
      };
      nextFeatures = Object.assign({}, currentFeatures, nextFeatures);
      await r
        .knex("organization")
        .update({
          features: JSON.stringify(nextFeatures)
        })
        .where({ id: organizationId });

      return await loaders.organization.load(organizationId);
    },

    createInvite: async (_, { user }) => {
      if ((user && user.is_superadmin) || !config.SUPPRESS_SELF_INVITE) {
        const [newInvite] = await r
          .knex("invite")
          .insert({
            is_valid: true,
            hash: uuidv4()
          })
          .returning("*");
        return newInvite;
      }
    },

    createCampaign: async (_, { campaign }, { user, loaders }) => {
      await accessRequired(
        user,
        campaign.organizationId,
        "ADMIN",
        /* allowSuperadmin=*/ true
      );

      await memoizer.invalidate(cacheOpts.CampaignsList.key, {
        organizationId: campaign.organizationId
      });

      const [origCampaignRecord] = await r
        .knex("campaign")
        .insert({
          organization_id: campaign.organizationId,
          creator_id: user.id,
          title: campaign.title,
          description: campaign.description,
          due_by: campaign.dueBy,
          is_started: false,
          is_archived: false
        })
        .returning("*");

      return editCampaign(
        origCampaignRecord.id,
        campaign,
        loaders,
        user,
        origCampaignRecord
      );
    },

    copyCampaign: async (_, { id }, { user, loaders }) => {
      const campaign = await loaders.campaign.load(id);
      await accessRequired(user, campaign.organization_id, "ADMIN");

      const result = await r.knex.transaction(async trx => {
        const [newCampaign] = await trx("campaign")
          .insert({
            organization_id: campaign.organization_id,
            creator_id: user.id,
            title: "COPY - " + campaign.title,
            description: campaign.description,
            due_by: campaign.dueBy,
            timezone: campaign.timezone,
            is_started: false,
            is_archived: false
          })
          .returning("*");
        const newCampaignId = newCampaign.id;
        const oldCampaignId = campaign.id;

        let interactions = await trx("interaction_step").where({
          campaign_id: oldCampaignId
        });

        const interactionsArr = [];
        interactions.forEach((interaction, index) => {
          if (interaction.parent_interaction_id) {
            let is = {
              id: "new" + interaction.id,
              questionText: interaction.question,
              scriptOptions: interaction.script_options,
              answerOption: interaction.answer_option,
              answerActions: interaction.answer_actions,
              isDeleted: interaction.is_deleted,
              campaign_id: newCampaignId,
              parentInteractionId: "new" + interaction.parent_interaction_id
            };
            interactionsArr.push(is);
          } else if (!interaction.parent_interaction_id) {
            let is = {
              id: "new" + interaction.id,
              questionText: interaction.question,
              scriptOptions: interaction.script_options,
              answerOption: interaction.answer_option,
              answerActions: interaction.answer_actions,
              isDeleted: interaction.is_deleted,
              campaign_id: newCampaignId,
              parentInteractionId: interaction.parent_interaction_id
            };
            interactionsArr.push(is);
          }
        });

        const interactionStepTree = makeTree(interactionsArr, (id = null));
        await persistInteractionStepTree(
          newCampaignId,
          interactionStepTree,
          campaign,
          trx
        );

        // Copy canned responses
        await trx.raw(
          `
            insert into canned_response (campaign_id, title, text)
            select
              ? as campaign_id,
              title,
              text
            from canned_response
            where campaign_id = ?
          `,
          [newCampaignId, oldCampaignId]
        );

        return newCampaign;
      });

      await memoizer.invalidate(cacheOpts.CampaignsList.key, {
        organizationId: campaign.organizationId
      });

      return result;
    },

    unarchiveCampaign: async (_, { id }, { user, loaders }) => {
      const { organization_id } = await loaders.campaign.load(id);
      await accessRequired(user, organization_id, "ADMIN");

      await memoizer.invalidate(cacheOpts.CampaignsList.key, {
        organizationId: organization_id
      });

      const [campaign] = await r
        .knex("campaign")
        .update({ is_archived: false })
        .where({ id })
        .returning("*");

      return campaign;
    },

    archiveCampaign: async (_, { id }, { user, loaders }) => {
      const { organization_id } = await loaders.campaign.load(id);
      await accessRequired(user, organization_id, "ADMIN");

      await memoizer.invalidate(cacheOpts.CampaignsList.key, {
        organizationId: organization_id
      });

      const [campaign] = await r
        .knex("campaign")
        .update({ is_archived: true })
        .where({ id })
        .returning("*");

      return campaign;
    },

    startCampaign: async (_, { id }, { user, loaders }) => {
      const { organization_id } = await loaders.campaign.load(id);
      await accessRequired(user, organization_id, "ADMIN");

      await memoizer.invalidate(cacheOpts.CampaignsList.key, {
        organizationId: organization_id
      });

      const [campaign] = await r
        .knex("campaign")
        .update({ is_started: true })
        .where({ id })
        .returning("*");

      return campaign;
    },

    editCampaign: async (
      _,
      { id, campaign: campaignEdits },
      { user, loaders }
    ) => {
      const origCampaign = await r
        .knex("campaign")
        .where({ id })
        .first();

      // Sometimes, campaign was coming through as having
      // a "null prototype", which caused .hasOwnProperty calls
      // to fail – this fixes it by ensuring its a proper object
      const campaign = Object.assign({}, campaignEdits);

      await accessRequired(user, origCampaign.organization_id, "ADMIN");

      memoizer.invalidate(cacheOpts.CampaignsList.key, {
        organizationId: campaign.organizationId
      });

      memoizer.invalidate(cacheOpts.CampaignOne.key, {
        campaignId: id
      });

      if (
        origCampaign.is_started &&
        campaign.hasOwnProperty("contacts") &&
        campaign.contacts
      ) {
        throw new GraphQLError(
          "Not allowed to add contacts after the campaign starts"
        );
      }
      return editCampaign(id, campaign, loaders, user, origCampaign);
    },

    filterLandlines: async (_, { id }, { user, loaders }) => {
      const campaign = await r
        .knex("campaign")
        .where({ id })
        .first();

      await accessRequired(user, campaign.organization_id, "ADMIN");

      if (campaign.is_started) {
        throw new GraphQLError(
          "Not allowed to filter landlines after the campaign starts"
        );
      }

      if (campaign.landlines_filtered) {
        throw new GraphQLError(
          "Landlines already filtered. You may need to wait for current contact upload to finish."
        );
      }

      const [job] = await r
        .knex("job_request")
        .insert({
          queue_name: `${id}:edit_campaign`,
          job_type: "filter_landlines",
          locks_queue: true,
          assigned: JOBS_SAME_PROCESS, // can get called immediately, below
          campaign_id: id,
          payload: ""
        })
        .returning("*");

      if (JOBS_SAME_PROCESS) {
        filterLandlines(job);
      }

      return loaders.campaign.load(id);
    },

    bulkUpdateScript: async (
      _,
      { organizationId, findAndReplace },
      { user, loaders }
    ) => {
      await accessRequired(user, organizationId, "OWNER");

      const scriptUpdatesResult = await r.knex.transaction(async trx => {
        const {
          searchString,
          replaceString,
          includeArchived,
          campaignTitlePrefixes
        } = findAndReplace;

        let campaignIdQuery = r
          .knex("campaign")
          .transacting(trx)
          .where({ organization_id: organizationId })
          .pluck("id");
        if (!includeArchived) {
          campaignIdQuery = campaignIdQuery.where({ is_archived: false });
        }
        if (campaignTitlePrefixes.length > 0) {
          campaignIdQuery = campaignIdQuery.where(function() {
            for (const prefix of campaignTitlePrefixes) {
              this.orWhere("title", "like", `${prefix}%`);
            }
          });
        }
        // TODO - MySQL Specific. This should be an inline subquery
        const campaignIds = await campaignIdQuery;

        // Using array_to_string is easier and faster than using unnest(script_options) (https://stackoverflow.com/a/7222285)
        const interactionStepsToChange = await r
          .knex("interaction_step")
          .transacting(trx)
          .select(["id", "campaign_id", "script_options"])
          .whereRaw("array_to_string(script_options, '||') like ?", [
            `%${searchString}%`
          ])
          .whereIn("campaign_id", campaignIds);

        const scriptUpdates = [];
        for (let step of interactionStepsToChange) {
          const script_options = step.script_options.map(scriptOption => {
            const newValue = replaceAll(
              scriptOption,
              searchString,
              replaceString
            );
            if (newValue !== scriptOption) {
              scriptUpdates.push({
                campaignId: step.campaign_id,
                found: scriptOption,
                replaced: newValue
              });
            }
            return newValue;
          });

          await r
            .knex("interaction_step")
            .transacting(trx)
            .update({ script_options })
            .where({ id: step.id });
        }

        return scriptUpdates;
      });

      return scriptUpdatesResult;
    },

    deleteJob: async (_, { campaignId, id }, { user, loaders }) => {
      const campaign = await r
        .knex("campaign")
        .where({ id: campaignId })
        .first();
      await accessRequired(user, campaign.organization_id, "ADMIN");
      await r.knex.transaction(async trx => {
        await trx("job_request")
          .where({
            id,
            campaign_id: campaignId
          })
          .delete();

        // Delete any associated Graphile Worker job
        await trx("graphile_worker.jobs")
          .whereRaw(`(payload->'__context'->>'job_request_id')::integer = ?`, [
            id
          ])
          .del();
      });
      return { id };
    },

    createCannedResponse: async (_, { cannedResponse }, { user, loaders }) => {
      authRequired(user);

      await r.knex("canned_response").insert({
        campaign_id: cannedResponse.campaignId,
        user_id: cannedResponse.userId,
        title: cannedResponse.title,
        text: cannedResponse.text
      });
      // deletes duplicate created canned_responses
      let query = r
        .knex("canned_response")
        .where(
          "text",
          "in",
          r
            .knex("canned_response")
            .where({
              text: cannedResponse.text,
              campaign_id: cannedResponse.campaignId
            })
            .select("text")
        )
        .andWhere({ user_id: cannedResponse.userId })
        .del();
      await query;
      cacheableData.cannedResponse.clearQuery({
        campaignId: cannedResponse.campaignId,
        userId: cannedResponse.userId
      });
    },

    createOrganization: async (
      _,
      { name, userId, inviteId },
      { loaders, user }
    ) => {
      authRequired(user);
      const invite = await loaders.invite.load(inviteId);
      if (!invite || !invite.is_valid) {
        throw new GraphQLError("That invitation is no longer valid");
      }

      const { payload = {} } = invite;

      const newOrganization = await r.knex.transaction(async trx => {
        const orgFeatures = {
          textRequestFormEnabled: false,
          textRequestType: TextRequestType.UNSENT,
          maxRequestCount: 100,
          defaulTexterApprovalStatus: RequestAutoApproveType.APPROVAL_REQUIRED
        };
        if (payload.org_features) {
          const { switchboard_lrn_api_key } = payload.org_features;
          if (switchboard_lrn_api_key) {
            orgFeatures.numbersApiKey = switchboard_lrn_api_key;
          }
        }

        const insertResult = await trx("organization")
          .insert({
            name,
            uuid: uuidv4(),
            features: JSON.stringify(orgFeatures)
          })
          .returning("*");

        const newOrganization = insertResult[0];

        const superadminIds = await trx("user")
          .where({ is_superadmin: true })
          .pluck("id");
        const ownerIds = new Set(superadminIds.concat([parseInt(userId)]));

        await trx("user_organization").insert(
          [...ownerIds].map(ownerId => ({
            role: "OWNER",
            user_id: ownerId,
            organization_id: newOrganization.id
          }))
        );

        await trx("invite")
          .update({
            is_valid: false
          })
          .where({
            id: parseInt(inviteId)
          });

        await trx("tag").insert({
          organization_id: newOrganization.id,
          title: "Escalated",
          description:
            "Escalation is meant for situations where you have exhausted all available help resources and still do not know how to respond.",
          confirmation_steps: [],
          is_assignable: false,
          is_system: true
        });

        if (payload.messaging_services) {
          await trx("messaging_service").insert(
            payload.messaging_services.map(service => ({
              messaging_service_sid: service.messaging_service_sid,
              organization_id: newOrganization.id,
              account_sid: service.account_sid,
              encrypted_auth_token: service.encrypted_auth_token,
              service_type: service.service_type
            }))
          );
        }

        return newOrganization;
      });

      return newOrganization;
    },

    editCampaignContactMessageStatus: async (
      _,
      { messageStatus, campaignContactId },
      { loaders, user }
    ) => {
      const contact = await loaders.campaignContact.load(campaignContactId);

      await assignmentRequiredOrHasOrgRoleForCampaign(user, contact.assignment_id, contact.campaign_id, 'SUPERVOLUNTEER');

      const [campaign] = await r
        .knex("campaign_contact")
        .update({ message_status: messageStatus })
        .where({ id: campaignContactId })
        .returning("*");
      return campaign;
    },

    getAssignmentContacts: async (
      _,
      { assignmentId, contactIds, findNew },
      { loaders, user }
    ) => {
      await assignmentRequired(user, assignmentId);

      const contacts = await r
        .knex("campaign_contact")
        .select("*")
        .whereIn("id", contactIds)
        .where({ assignment_id: assignmentId });

      const messages = await r
        .knex("message")
        .select(
          "id",
          "text",
          "is_from_contact",
          "created_at",
          "campaign_contact_id"
        )
        .whereIn("campaign_contact_id", contactIds)
        .orderBy("created_at", "asc");

      const messagesByContactId = groupBy(messages, x => x.campaign_contact_id);

      const shouldFetchTagsAndQuestionResponses =
        contacts.filter(c => c.message_status !== "needsMessage").length > 0;

      const tags = shouldFetchTagsAndQuestionResponses
        ? await r
            .knex("tag")
            .select("tag.*")
            .select("campaign_contact_id")
            .join(
              "campaign_contact_tag",
              "campaign_contact_tag.tag_id",
              "=",
              "tag.id"
            )
            .whereIn("campaign_contact_tag.campaign_contact_id", contactIds)
        : [];

      const tagsByContactId = groupBy(tags, x => x.campaign_contact_id);

      const questionResponses = shouldFetchTagsAndQuestionResponses
        ? await r
            .knex("question_response")
            .join(
              "interaction_step as istep",
              "question_response.interaction_step_id",
              "istep.id"
            )
            .whereIn("question_response.campaign_contact_id", contactIds)
            .select(
              "value",
              "interaction_step_id",
              "istep.question as istep_question",
              "istep.id as istep_id",
              "campaign_contact_id"
            )
        : [];

      const questionResponsesByContactId = groupBy(
        questionResponses,
        x => x.campaign_contact_id
      );

      const contactsById = contacts.reduce(
        (acc, c) =>
          Object.assign(acc, {
            [c.id]: {
              ...c,
              messages: messagesByContactId[c.id] || [],
              contactTags: tagsByContactId[c.id] || [],
              questionResponseValues: (
                questionResponsesByContactId[c.id] || []
              ).map(qr => ({
                value: qr.value,
                interaction_step_id: qr.interaction_step_id,
                id: qr.interaction_step_id,
                question: qr.istep_question
              }))
            }
          }),
        {}
      );

      return contactIds.map(cid => contactsById[cid]);
    },

    findNewCampaignContact: async (
      _,
      { assignmentId, numberContacts },
      { loaders, user }
    ) => {
      // TODO: re-enable once dynamic assignment is fixed (#548)
      throw new GraphQLError("Invalid assignment");
      /* This attempts to find a new contact for the assignment, in the case that useDynamicAssigment == true */
      const assignment = await r
        .knex("assignment")
        .where({ id: assignmentId })
        .first();
      if (assignment.user_id != user.id) {
        throw new GraphQLError("Invalid assignment");
      }
      const campaign = await r
        .knex("campaign")
        .where({ id: assignment.campaign_id })
        .first();
      if (!campaign.use_dynamic_assignment || assignment.max_contacts === 0) {
        return { found: false };
      }

      const contactsCount = await r.getCount(
        r
          .knex("campaign_contact")
          .where({ assignment_id: assignmentId })
          .whereRaw("archived = false") // partial index friendly
      );

      numberContacts = numberContacts || 1;
      if (
        assignment.max_contacts &&
        contactsCount + numberContacts > assignment.max_contacts
      ) {
        numberContacts = assignment.max_contacts - contactsCount;
      }
      // Don't add more if they already have that many
      const result = await r.getCount(
        r
          .knex("campaign_contact")
          .where({
            assignment_id: assignmentId,
            message_status: "needsMessage",
            is_opted_out: false
          })
          .whereRaw("archived = false") // partial index friendly
      );

      if (result >= numberContacts) {
        return { found: false };
      }

      const updateResult = await r
        .knex("campaign_contact")
        .where(
          "id",
          "in",
          r
            .knex("campaign_contact")
            .where({
              assignment_id: null,
              campaign_id: campaign.id
            })
            .whereRaw("archived = false") // partial index friendly
            .limit(numberContacts)
            .select("id")
        )
        .update({ assignment_id: assignmentId })
        .catch(logger.error);

      if (updateResult > 0) {
        return { found: true };
      } else {
        return { found: false };
      }
    },
    tagConversation: async (_, { campaignContactId, tag }, { user }) => {
      const campaignContact = await r
        .knex("campaign_contact")
        .join("campaign", "campaign.id", "campaign_contact.campaign_id")
        .where({ "campaign_contact.id": campaignContactId })
        .first(["campaign_contact.*", "campaign.organization_id"]);
      try {
        await assignmentRequired(user, campaignContact.assignment_id);
      } catch (err) {
        accessRequired(user, campaignContact.organization_id, "SUPERVOLUNTEER");
      }

      const { addedTagIds, removedTagIds } = tag;
      const tagsToInsert = addedTagIds.map(tagId => ({
        campaign_contact_id: campaignContactId,
        tag_id: tagId,
        tagger_id: user.id
      }));
      const [deleteResult, insertResult] = await Promise.all([
        await r
          .knex("campaign_contact_tag")
          .where({ campaign_contact_id: parseInt(campaignContactId) })
          .whereIn("tag_id", removedTagIds)
          .del(),
        await r.knex("campaign_contact_tag").insert(tagsToInsert)
      ]);

      // See if any of the newly applied tags are is_assignable = false
      const newlyAssignedTagsThatShouldUnassign = await r
        .knex("tag")
        .select("id")
        .whereIn("id", addedTagIds)
        .where({ is_assignable: false });

      const currentlyEscalating =
        newlyAssignedTagsThatShouldUnassign.length > 0;

      if (tag.message) {
        try {
          const checkOptOut = true;
          const checkAssignment = false;
          await sendMessage(
            user,
            campaignContactId,
            tag.message,
            checkOptOut,
            checkAssignment,
            currentlyEscalating
          );
        } catch (error) {
          // Log the sendMessage error, but return successful opt out creation
          logger.error("Error sending message for tag", error);
        }
      }

      const webhookUrls = await r
        .knex("tag")
        .whereIn("id", addedTagIds)
        .pluck("webhook_url")
        .then(urls => urls.filter(url => url.length > 0));

      await notifyOnTagConversation(campaignContactId, user.id, webhookUrls);

      if (currentlyEscalating) {
        await r
          .knex("campaign_contact")
          .update({ assignment_id: null })
          .where({ id: parseInt(campaignContactId) });
      }

      return campaignContact;
    },
    createOptOut: async (
      _,
      { optOut, campaignContactId },
      { loaders, user }
    ) => {
      const contact = await loaders.campaignContact.load(campaignContactId);
      let organizationId = contact.organization_id;
      if (!organizationId) {
        const campaign = await loaders.campaign.load(contact.campaign_id);
        organizationId = campaign.organization_id;
      }
      try {
        await assignmentRequired(user, contact.assignment_id);
      } catch (error) {
        await accessRequired(user, organizationId, "SUPERVOLUNTEER");
      }

      let { assignmentId, cell, message, reason } = optOut;
      if (!assignmentId) {
        // Check for existing assignment
        const assignment = await r
          .knex("assignment")
          .where({
            user_id: user.id,
            campaign_id: contact.campaign_id
          })
          .first("id");
        if (assignment && assignment.id) {
          assignmentId = assignment.id;
        } else {
          // Create assignment if no exisiting
          const [newAssignment] = await r
            .knex("assignment")
            .insert({
              user_id: user.id,
              campaign_id: contact.campaign_id
            })
            .returning("*");
          eventBus.emit(EventType.AssignmentCreated, newAssignment);
          assignmentId = newAssignment.id;
        }
      }

      await cacheableData.optOut.save({
        cell,
        reason,
        assignmentId,
        organizationId
      });

      if (message) {
        const checkOptOut = false;
        try {
          await sendMessage(user, campaignContactId, message, checkOptOut);
        } catch (error) {
          // Log the sendMessage error, but return successful opt out creation
          logger.error("Error sending message for opt-out", error);
        }
      }

      // Force reload with updated `is_opted_out` status
      loaders.campaignContact.clear(campaignContactId);
      return loaders.campaignContact.load(campaignContactId);
    },

    removeOptOut: async (_, { cell }, { loaders, user }) => {
      // We assume that OptOuts are shared across orgs
      // const sharingOptOuts = config.OPTOUTS_SHARE_ALL_ORGS

      // Authorization (checking across all organizations)
      let userRoles = await r
        .knex("user_organization")
        .where({ user_id: user.id })
        .select("role");
      userRoles = userRoles.map(role => role.role);
      userRoles = Array.from(new Set(userRoles));
      const isAdmin = hasRole("SUPERVOLUNTEER", userRoles);
      if (!isAdmin) {
        throw new GraphQLError(
          "You are not authorized to access that resource."
        );
      }

      const contactIds = await r.knex.transaction(async trx => {
        // Remove all references in the opt out table
        const optOuts = r
          .knex("opt_out")
          .transacting(trx)
          .where({ cell })
          .del();

        // Update all "cached" values for campaign contacts
        // TODO - MySQL Specific. Fetching contactIds can be done in a subquery
        const contactUpdates = r
          .knex("campaign_contact")
          .transacting(trx)
          .leftJoin("campaign", "campaign_contact.campaign_id", "campaign.id")
          .where({
            "campaign_contact.cell": cell,
            "campaign.is_archived": false
          })
          .pluck("campaign_contact.id")
          .then(contactIds => {
            return (
              r
                .knex("campaign_contact")
                .transacting(trx)
                .whereIn("id", contactIds)
                .update({ is_opted_out: false })
                // Return updated contactIds from Promise chain
                .then(_ => contactIds)
            );
          });

        const [_optOutRes, contactIds] = await Promise.all([
          optOuts,
          contactUpdates
        ]);
        return contactIds;
      });

      // We don't care about Redis
      // await cacheableData.optOut.clearCache(...)

      return contactIds.map(contactId => ({
        id: contactId,
        is_opted_out: false
      }));
    },

    bulkSendMessages: async (_, { assignmentId }, loaders) => {
      if (!config.ALLOW_SEND_ALL || !config.NOT_IN_USA) {
        logger.error("Not allowed to send all messages at once");
        throw new GraphQLError("Not allowed to send all messages at once");
      }

      const assignment = await r
        .knex("assignment")
        .where({ id: assignmentId })
        .first();
      // Assign some contacts
      await rootMutations.RootMutation.findNewCampaignContact(
        _,
        {
          assignmentId,
          numberContacts: Number(config.BULK_SEND_CHUNK_SIZE) - 1
        },
        loaders
      );

      const contacts = await r
        .knex("campaign_contact")
        .where({
          message_status: "needsMessage",
          assignment_id: assignmentId
        })
        .whereRaw("archived = false") // partial index friendly
        .orderByRaw("updated_at")
        .limit(config.BULK_SEND_CHUNK_SIZE);

      const texter = camelCaseKeys(
        await r
          .knex("user")
          .where({ id: assignment.user_id })
          .first()
      );
      const customFields = Object.keys(JSON.parse(contacts[0].custom_fields));

      const contactMessages = await contacts.map(async contact => {
        const script = await campaignContactResolvers.CampaignContact.currentInteractionStepScript(
          contact
        );
        contact.customFields = contact.custom_fields;
        const text = applyScript({
          contact: camelCaseKeys(contact),
          texter,
          script,
          customFields
        });
        const contactMessage = {
          contactNumber: contact.cell,
          userId: assignment.user_id,
          text,
          assignmentId
        };
        await rootMutations.RootMutation.sendMessage(
          _,
          { message: contactMessage, campaignContactId: contact.id },
          loaders
        );
      });

      return [];
    },

    sendMessage: async (
      _,
      { message, campaignContactId },
      { user, loaders }
    ) => {
      return await sendMessage(user, campaignContactId, message);
    },

    deleteQuestionResponses: async (
      _,
      { interactionStepIds, campaignContactId },
      { loaders, user }
    ) => {
      const contact = await loaders.campaignContact.load(campaignContactId);
      try {
        await assignmentRequired(user, contact.assignment_id);
      } catch (error) {
        const campaign = await r
          .knex("campaign")
          .where({ id: contact.campaign_id })
          .first();
        const organizationId = campaign.organization_id;
        await accessRequired(user, organizationId, "SUPERVOLUNTEER");
      }
      // TODO: maybe undo action_handler
      await r
        .knex("question_response")
        .where({ campaign_contact_id: campaignContactId })
        .whereIn("interaction_step_id", interactionStepIds)
        .del();
      return contact;
    },

    updateQuestionResponses: async (
      _,
      { questionResponses, campaignContactId },
      { loaders }
    ) => {
      // TODO: wrap in transaction
      const count = questionResponses.length;

      for (let i = 0; i < count; i++) {
        const questionResponse = questionResponses[i];
        const { interactionStepId, value } = questionResponse;
        await r
          .knex("question_response")
          .where({
            campaign_contact_id: campaignContactId,
            interaction_step_id: interactionStepId
          })
          .del();

        // TODO: maybe undo action_handler if updated answer

        const [qr] = await r
          .knex("question_response")
          .insert({
            campaign_contact_id: campaignContactId,
            interaction_step_id: interactionStepId,
            value
          })
          .returning("*");
        const interactionStepResult = await r
          .knex("interaction_step")
          // TODO: is this really parent_interaction_id or just interaction_id?
          .where({
            parent_interaction_id: interactionStepId,
            answer_option: value
          })
          .whereNot("answer_actions", "")
          .whereNotNull("answer_actions");

        const interactionStepAction =
          interactionStepResult.length &&
          interactionStepResult[0].answer_actions;
        if (interactionStepAction) {
          // run interaction step handler
          try {
            const handler = require(`../action_handlers/${interactionStepAction}.js`);
            handler.processAction(
              qr,
              interactionStepResult[0],
              campaignContactId
            );
          } catch (err) {
            logger.error("Handler for InteractionStep does not exist", {
              error: errToObj(err),
              interactionStepId,
              interactionStepAction
            });
          }
        }
      }

      const contact = loaders.campaignContact.load(campaignContactId);
      return contact;
    },

    markForSecondPass: async (
      _ignore,
      { campaignId, excludeAgeInHours },
      { user }
    ) => {
      // verify permissions
      const campaign = await r
        .knex("campaign")
        .where({ id: parseInt(campaignId) })
        .first(["organization_id", "is_archived"]);

      const organizationId = campaign.organization_id;

      await accessRequired(user, organizationId, "ADMIN", true);

      const queryArgs = [parseInt(campaignId)];
      if (excludeAgeInHours) {
        queryArgs.push(parseFloat(excludeAgeInHours));
      }

      /**
       * "Mark Campaign for Second Pass", will only mark contacts for a second
       * pass that do not have a more recently created membership in another campaign.
       * Using SQL injection to avoid passing archived as a binding
       * Should help with guaranteeing partial index usage
       */
      const updateResultRaw = await r.knex.raw(
        `
        update
          campaign_contact as current_contact
        set
          message_status = 'needsMessage'
        where current_contact.campaign_id = ?
          and current_contact.message_status = 'messaged'
          and current_contact.archived = ${campaign.is_archived}
          and not exists (
            select
              cell
            from
              campaign_contact as newer_contact
            where
              newer_contact.cell = current_contact.cell
              and newer_contact.created_at > current_contact.created_at
          )
          and not exists (
            select 1
            from message
            where current_contact.id = message.campaign_contact_id
              and is_from_contact = true
          )
          ${
            excludeAgeInHours
              ? "and current_contact.updated_at < now() - interval '?? hour'"
              : ""
          }
        ;
      `,
        queryArgs
      );

      const updateResult = updateResultRaw.rowCount;

      return `Marked ${updateResult} campaign contacts for a second pass.`;
    },

    unMarkForSecondPass: async (_ignore, { campaignId }, { user }) => {
      // verify permissions
      const campaign = await r
        .knex("campaign")
        .where({ id: parseInt(campaignId) })
        .first(["organization_id", "is_archived"]);

      const organizationId = campaign.organization_id;

      await accessRequired(user, organizationId, "ADMIN", true);

      /**
       * "Un-Mark Campaign for Second Pass", will only mark contacts as messaged
       * if they are currently needsMessage and have been sent a message and have not replied
       *
       * Using SQL injection to avoid passing archived as a binding
       * Should help with guaranteeing partial index usage
       */
      const updateResultRaw = await r.knex.raw(
        `
        update
          campaign_contact
        set
          message_status = 'messaged'
        where campaign_contact.campaign_id = ?
          and campaign_contact.message_status = 'needsMessage'
          and campaign_contact.archived = ${campaign.is_archived}
          and exists (
            select 1
            from message
            where message.campaign_contact_id = campaign_contact.id
              and is_from_contact = false
          ) 
          and not exists (
            select 1
            from message
            where message.campaign_contact_id = campaign_contact.id
              and is_from_contact = true
          )
        ;
      `,
        [parseInt(campaignId)]
      );

      const updateResult = updateResultRaw.rowCount;

      return `Un-Marked ${updateResult} campaign contacts for a second pass.`;
    },

    deleteNeedsMessage: async (_ignore, { campaignId }, { user }) => {
      // verify permissions
      const campaign = await r
        .knex("campaign")
        .where({ id: parseInt(campaignId) })
        .first(["organization_id", "is_archived"]);

      const organizationId = campaign.organization_id;

      await accessRequired(user, organizationId, "ADMIN", true);

      /**
       * deleteNeedsMessage will only delete contacts
       * if they are currently needsMessage and have NOT been sent a message
       *
       * Using SQL injection to avoid passing archived as a binding
       * Should help with guaranteeing partial index usage
       */
      const deleteResult = await r.knex.raw(
        `
        delete from campaign_contact
        where campaign_contact.campaign_id = ?
          and campaign_contact.message_status = 'needsMessage'
          and campaign_contact.archived = ${campaign.is_archived}
          and not exists (
            select 1
            from message
            where message.campaign_contact_id = campaign_contact.id
          )
        ;
      `,
        [parseInt(campaignId)]
      );

      const updateResult = deleteResult.rowCount;

      return `Deleted ${updateResult} unmessaged campaign contacts`;
    },

    insertLinkDomain: async (
      _ignore,
      { organizationId, domain, maxUsageCount },
      { user }
    ) => {
      // verify permissions
      await accessRequired(user, organizationId, "OWNER", /* superadmin*/ true);

      const insertResult = await r
        .knex("link_domain")
        .insert({
          organization_id: organizationId,
          max_usage_count: maxUsageCount,
          domain
        })
        .returning("*");

      return insertResult[0];
    },

    updateLinkDomain: async (
      _ignore,
      { organizationId, domainId, payload },
      { user }
    ) => {
      // verify permissions
      await accessRequired(user, organizationId, "OWNER", /* superadmin*/ true);

      const { maxUsageCount, isManuallyDisabled } = payload;
      if (maxUsageCount === undefined && isManuallyDisabled === undefined)
        throw new Error("Must supply at least one field to update.");

      let query = r
        .knex("link_domain")
        .where({
          id: domainId,
          organization_id: organizationId
        })
        .returning("*");
      if (maxUsageCount !== undefined)
        query = query.update({ max_usage_count: maxUsageCount });
      if (isManuallyDisabled !== undefined)
        query = query.update({ is_manually_disabled: isManuallyDisabled });

      const linkDomainResult = await query;
      return linkDomainResult[0];
    },

    deleteLinkDomain: async (
      _ignore,
      { organizationId, domainId },
      { user }
    ) => {
      // verify permissions
      await accessRequired(user, organizationId, "OWNER", /* superadmin*/ true);

      await r
        .knex("link_domain")
        .where({
          id: domainId,
          organization_id: organizationId
        })
        .del();

      return true;
    },

    megaReassignCampaignContacts: async (
      _ignore,
      { organizationId, campaignIdsContactIds, newTexterUserIds },
      { user }
    ) => {
      // verify permissions
      await accessRequired(user, organizationId, "ADMIN", /* superadmin*/ true);

      if (newTexterUserIds == null) {
        const campaignContactIdsToUnassign = campaignIdsContactIds.map(
          cc => cc.campaignContactId
        );

        await r
          .knex("campaign_contact")
          .update({ assignment_id: null })
          .whereIn("id", campaignContactIdsToUnassign);

        return true;
      }

      // group contactIds by campaign
      // group messages by campaign
      const aggregated = {};
      campaignIdsContactIds.forEach(campaignIdContactId => {
        aggregated[campaignIdContactId.campaignContactId] = {
          campaign_id: campaignIdContactId.campaignId,
          messages: campaignIdContactId.messageIds
        };
      });

      const result = Object.entries(aggregated);
      const numberOfCampaignContactsToReassign = result.length;
      const numberOfCampaignContactsPerNextTexter = Math.ceil(
        numberOfCampaignContactsToReassign / newTexterUserIds.length
      );
      const chunks = _.chunk(result, numberOfCampaignContactsPerNextTexter);

      for (let [idx, chunk] of chunks.entries()) {
        const byCampaignId = _.groupBy(chunk, x => x[1].campaign_id);
        const campaignIdContactIdsMap = new Map();
        const campaignIdMessageIdsMap = new Map();

        Object.keys(byCampaignId).forEach(campaign_id => {
          chunk.filter(x => x[1].campaign_id === campaign_id).forEach(x => {
            if (!campaignIdContactIdsMap.has(campaign_id))
              campaignIdContactIdsMap.set(campaign_id, []);
            if (!campaignIdMessageIdsMap.has(campaign_id))
              campaignIdMessageIdsMap.set(campaign_id, []);
            campaignIdContactIdsMap.get(campaign_id).push(x[0]);
            x[1].messages.forEach(message_id => {
              campaignIdMessageIdsMap.get(campaign_id).push(message_id);
            });
          });
        });

        await reassignConversations(
          campaignIdContactIdsMap,
          campaignIdMessageIdsMap,
          newTexterUserIds[idx]
        );
      }

      return true;
    },

    megaBulkReassignCampaignContacts: async (
      _ignore,
      {
        organizationId,
        campaignsFilter,
        assignmentsFilter,
        tagsFilter,
        contactsFilter,
        newTexterUserIds
      },
      { user }
    ) => {
      // verify permissions
      await accessRequired(user, organizationId, "ADMIN", /* superadmin*/ true);

      const campaignContactIdsToMessageIds = await getCampaignIdMessageIdsAndCampaignIdContactIdsMapsChunked(
        organizationId,
        campaignsFilter,
        assignmentsFilter,
        tagsFilter,
        contactsFilter
      );

      if (newTexterUserIds == null) {
        const campaignContactIdsToUnassign = campaignContactIdsToMessageIds.map(
          ([ccId, _]) => ccId
        );

        await r
          .knex("campaign_contact")
          .update({ assignment_id: null })
          .whereIn("id", campaignContactIdsToUnassign);

        return true;
      }

      const numberOfCampaignContactsToReassign =
        campaignContactIdsToMessageIds.length;
      const numberOfCampaignContactsPerNextTexter = Math.ceil(
        numberOfCampaignContactsToReassign / newTexterUserIds.length
      );
      const chunks = _.chunk(
        campaignContactIdsToMessageIds,
        numberOfCampaignContactsPerNextTexter
      );
      for (let [idx, chunk] of chunks.entries()) {
        const byCampaignId = _.groupBy(chunk, x => x[1].campaign_id);
        const campaignIdContactIdsMap = new Map();
        const campaignIdMessageIdsMap = new Map();

        Object.keys(byCampaignId).forEach(campaign_id => {
          chunk
            .filter(x => x[1].campaign_id === parseInt(campaign_id))
            .forEach(x => {
              if (!campaignIdContactIdsMap.has(campaign_id))
                campaignIdContactIdsMap.set(campaign_id, []);
              if (!campaignIdMessageIdsMap.has(campaign_id))
                campaignIdMessageIdsMap.set(campaign_id, []);
              campaignIdContactIdsMap.get(campaign_id).push(x[0]);
              x[1].messages.forEach(message_id => {
                campaignIdMessageIdsMap.get(campaign_id).push(message_id);
              });
            });
        });

        await reassignConversations(
          campaignIdContactIdsMap,
          campaignIdMessageIdsMap,
          newTexterUserIds[idx]
        );
      }

      return true;
    },

    requestTexts: async (
      _,
      { count, organizationId, preferredTeamId },
      { user, loaders }
    ) => {
      const myAssignmentTarget = await myCurrentAssignmentTarget(
        user.id,
        organizationId
      );

      if (!myAssignmentTarget) {
        return "No texts available at the moment";
      }

      return r.knex.transaction(async trx => {
        const [pendingAssignmentRequest] = await trx("assignment_request")
          .insert({
            user_id: user.id,
            organization_id: organizationId,
            amount: count,
            preferred_team_id: preferredTeamId
          })
          .returning("*");

        const { request_status } = await trx("user_organization")
          .where({
            user_id: user.id,
            organization_id: organizationId
          })
          .first(["request_status"]);

        // Only trigger webhook if approval is required (may want to allow expanding list in future)
        if (["approval_required"].includes(request_status)) {
          await notifyAssignmentCreated({
            userId: user.id,
            organizationId,
            count
          }).catch(err => {
            logger.error("Error submitting external assignment request: ", err);

            if (config.ASSIGNMENT_REQUESTED_URL_REQUIRED) {
              const message = err.response
                ? err.response.body.message
                : err.message;
              throw new Error(`Could not submit external requst: ${message}`);
            }
          });
        }

        if (config.AUTO_HANDLE_REQUESTS) {
          const worker = await getWorker();
          await worker.addJob(
            "handle-autoassignment-request",
            pendingAssignmentRequest
          );
        }

        return "Created";
      });
    },
    releaseMessages: async (
      _,
      { campaignId, target, ageInHours },
      { user }
    ) => {
      let messageStatus;
      switch (target) {
        case "UNSENT":
          messageStatus = "needsMessage";
          break;
        case "UNREPLIED":
          messageStatus = "needsResponse";
          break;

        default:
          throw new Error(`Unknown ReleaseActionTarget '${target}'`);
      }

      let ageInHoursAgo;
      if (!!ageInHours) {
        ageInHoursAgo = new Date();
        ageInHoursAgo.setHours(new Date().getHours() - ageInHours);
        ageInHoursAgo = ageInHoursAgo.toISOString();
      }

      const campaign = await r
        .knex("campaign")
        .where({ id: campaignId })
        .first(["organization_id", "is_archived"]);

      const updatedCount = await r.knex.transaction(async trx => {
        const queryArgs = [parseInt(campaignId), messageStatus];
        if (ageInHours) queryArgs.push(ageInHoursAgo);

        /**
         * Using SQL injection to avoid passing archived as a binding
         * Should help with guaranteeing partial index usage
         */
        const rawResult = await trx.raw(
          `
          update
            campaign_contact
          set
            assignment_id = null
          from
            assignment, campaign
          where
            campaign_contact.campaign_id = ?
            and campaign.id = campaign_contact.campaign_id
            and assignment.id = campaign_contact.assignment_id
            and is_opted_out = false
            and message_status = ?
            and archived = ${campaign.is_archived}
            and not exists (
              select 1 
              from campaign_contact_tag
              join tag on tag.id = campaign_contact_tag.tag_id
              where tag.is_assignable = false
                and campaign_contact_tag.campaign_contact_id = campaign_contact.id
            )
            ${ageInHours ? "and campaign_contact.updated_at < ?" : ""}
        `,
          queryArgs
        );

        return rawResult.rowCount;
      });

      return `Released ${updatedCount} ${target.toLowerCase()} messages for reassignment`;
    },
    releaseAllUnhandledReplies: async (
      _,
      {
        organizationId,
        ageInHours,
        releaseOnRestricted,
        limitToCurrentlyTextableContacts
      },
      { user }
    ) => {
      await accessRequired(user, organizationId, "ADMIN", true);

      const releaseOnLimitAssignmentToTeams =
        releaseOnRestricted != null ? releaseOnRestricted : false;

      const limitToIsTextableNow =
        limitToCurrentlyTextableContacts != null
          ? limitToCurrentlyTextableContacts
          : true;

      /*
       * Using SQL injection to avoid passing archived as a binding
       * Should help with guaranteeing partial index usage
       */
      const rawResult = await r.knex.raw(
        `
          with update_result as (
            update
              campaign_contact
            set
              assignment_id = null
            from
              campaign
            where
              campaign_contact.campaign_id = campaign.id
              and campaign.organization_id = ?
              and (? or campaign.limit_assignment_to_teams = false)
              and not exists (
                select 1
                from message
                where is_from_contact = false
                  and campaign_contact_id = campaign_contact.id
                  and created_at > now() - (? * interval '1 hours')
              )
              and is_opted_out = false
              and message_status = 'needsResponse'
              and archived = false
              and not exists (
                select 1 
                from campaign_contact_tag
                join tag on tag.id = campaign_contact_tag.tag_id
                where tag.is_assignable = false
                  and campaign_contact_tag.campaign_contact_id = campaign_contact.id
              )
              and (
                ? or contact_is_textable_now(
                  coalesce(campaign_contact.timezone, spoke_tz_to_iso_tz(campaign.timezone)),
                  campaign.texting_hours_start,
                  campaign.texting_hours_end,
                  extract('hour' from current_timestamp at time zone campaign.timezone) < campaign.texting_hours_end
                  and 
                  extract('hour' from current_timestamp at time zone campaign.timezone) > campaign.texting_hours_start
                )
              )
            returning 1, campaign_id
          )
          select
            count(*) as contact_count,
            count(distinct campaign_id) as campaign_count
          from
            update_result
        `,
        [
          parseInt(organizationId),
          releaseOnLimitAssignmentToTeams,
          ageInHours || 0,
          limitToIsTextableNow
        ]
      );

      const result = rawResult.rows[0];
      return {
        contactCount: result.contact_count,
        campaignCount: result.campaign_count
      };
    },
    deleteCampaignOverlap: async (
      _,
      { organizationId, campaignId, overlappingCampaignId },
      { user }
    ) => {
      await accessRequired(user, organizationId, "ADMIN", /* superadmin*/ true);

      const { deletedRowCount, remainingCount } = await r.knex.transaction(
        async trx => {
          // Get total count, including second pass contacts, locking for subsequent delete
          let remainingCount = await queryCampaignOverlapCount(
            campaignId,
            overlappingCampaignId,
            trx
          );

          // Delete, excluding second pass contacts that have already been messaged
          const { rowCount: deletedRowCount } = await trx.raw(
            `
            delete from
              campaign_contact
            where
              campaign_contact.campaign_id = ?
              and not exists (
                select 1
                from message
                where campaign_contact_id = campaign_contact.id
              )
              and exists (
                select 1
                from campaign_contact as other_campaign_contact
                where other_campaign_contact.campaign_id = ?
                  and other_campaign_contact.cell = campaign_contact.cell
              );`,
            [campaignId, overlappingCampaignId]
          );

          remainingCount = remainingCount - deletedRowCount;

          return { deletedRowCount, remainingCount };
        }
      );

      return {
        campaign: { id: overlappingCampaignId },
        deletedRowCount,
        remainingCount
      };
    },
    deleteManyCampaignOverlap: async (
      _,
      { organizationId, campaignId, overlappingCampaignIds },
      { user }
    ) => {
      await accessRequired(user, organizationId, "ADMIN", /* superadmin*/ true);

      // Delete, excluding second pass contacts that have already been messaged
      const { rowCount: deletedRowCount } = await r.knex.raw(
        `
        delete from
          campaign_contact
        where
          campaign_contact.campaign_id = ?
          and not exists (
            select 1
            from message
            where campaign_contact_id = campaign_contact.id
          )
          and exists (
            select 1
            from campaign_contact as other_campaign_contact
            where other_campaign_contact.campaign_id = ANY(?)
              and other_campaign_contact.cell = campaign_contact.cell
          );`,
        [campaignId, overlappingCampaignIds]
      );

      return deletedRowCount;
    },
    resolveAssignmentRequest: async (
      _,
      { assignmentRequestId, approved, autoApproveLevel },
      { user }
    ) => {
      assignmentRequestId = parseInt(assignmentRequestId);
      const assignmentRequest = await r
        .knex("assignment_request")
        .first("*")
        .where({ id: assignmentRequestId });

      if (!assignmentRequest) {
        throw new Error("Assignment request not found");
      }

      const roleRequired = autoApproveLevel ? "ADMIN" : "SUPERVOLUNTEER";
      await accessRequired(
        user,
        assignmentRequest.organization_id,
        roleRequired
      );

      const numberAssigned = await r.knex.transaction(async trx => {
        if (autoApproveLevel) {
          await trx("user_organization")
            .where({
              user_id: assignmentRequest.user_id,
              organization_id: assignmentRequest.organization_id
            })
            .update({ request_status: autoApproveLevel.toLowerCase() });
        }

        await trx("assignment_request")
          .update({
            status: approved ? "approved" : "rejected",
            approved_by_user_id: user.id
          })
          .where({ id: assignmentRequestId });

        if (!approved) return 0;

        const countUpdated = await giveUserMoreTexts(
          assignmentRequest.user_id,
          assignmentRequest.amount,
          assignmentRequest.organization_id,
          assignmentRequest.preferred_team_id,
          trx
        );
        return countUpdated;
      });

      return numberAssigned;
    },
    saveTag: async (_, { organizationId, tag }, { user }) => {
      await accessRequired(user, organizationId, "ADMIN");

      // Update existing tag
      if (tag.id) {
        const [updatedTag] = await r
          .knex("tag")
          .update({
            title: tag.title,
            description: tag.description,
            is_assignable: tag.isAssignable,
            on_apply_script: tag.onApplyScript
          })
          .where({
            id: tag.id,
            organization_id: organizationId,
            is_system: false
          })
          .returning("*");
        if (!updatedTag) throw new Error("No matching tag to update!");
        return updatedTag;
      }

      // Create new tag
      const [newTag] = await r
        .knex("tag")
        .insert({
          organization_id: organizationId,
          author_id: user.id,
          title: tag.title,
          description: tag.description,
          is_assignable: tag.isAssignable,
          on_apply_script: tag.onApplyScript
        })
        .returning("*");

      memoizer.invalidate(cacheOpts.OrganizationTagList.key, {
        organizationId
      });

      memoizer.invalidate(cacheOpts.OrganizationEscalatedTagList.key, {
        organizationId
      });

      return newTag;
    },
    deleteTag: async (_, { organizationId, tagId }, { user }) => {
      await accessRequired(user, organizationId, "ADMIN");

      const deleteCount = await r
        .knex("tag")
        .where({
          id: tagId,
          organization_id: organizationId,
          is_system: false
        })
        .del();
      if (deleteCount !== 1) throw new Error("Could not delete the tag.");

      memoizer.invalidate(cacheOpts.OrganizationTagList.key, {
        organizationId
      });

      memoizer.invalidate(cacheOpts.OrganizationEscalatedTagList.key, {
        organizationId
      });

      return true;
    },
    saveTeams: async (_, { organizationId, teams }, { user }) => {
      await accessRequired(user, organizationId, "ADMIN");

      const stripUndefined = obj => {
        const result = { ...obj };
        Object.keys(result).forEach(
          key => result[key] === undefined && delete result[key]
        );
        return result;
      };

      const updatedTeams = await r.knex.transaction(async trx => {
        const isTeamOrg = team => team.id && team.id === "general";
        const orgTeam = teams.find(isTeamOrg);

        if (orgTeam) {
          let { features: currentFeatures } = await trx("organization")
            .where({ id: organizationId })
            .first("features");

          try {
            currentFeatures = JSON.parse(currentFeatures);
          } catch (_ex) {
            currentFeatures = {};
          }

          let nextFeatures = stripUndefined({
            textRequestFormEnabled: orgTeam.isAssignmentEnabled,
            textRequestType: orgTeam.assignmentType,
            textRequestMaxCount: orgTeam.maxRequestCount
          });
          nextFeatures = Object.assign({}, currentFeatures, nextFeatures);
          await trx("organization")
            .update({ features: JSON.stringify(nextFeatures) })
            .where({ id: organizationId });
        }

        const nonOrgTeams = teams.filter(team => !isTeamOrg(team));

        return Promise.all(
          nonOrgTeams.map(async team => {
            const payload = stripUndefined({
              title: team.title,
              description: team.description,
              text_color: team.textColor,
              background_color: team.backgroundColor,
              is_assignment_enabled: team.isAssignmentEnabled,
              assignment_priority: team.assignmentPriority,
              assignment_type: team.assignmentType,
              max_request_count: team.maxRequestCount
            });

            let teamToReturn;

            // Update existing team
            // true for updating fields on the team itself
            if (team.id && Object.keys(payload).length > 0) {
              const [updatedTeam] = await trx("team")
                .update(payload)
                .where({
                  id: team.id,
                  organization_id: organizationId
                })
                .returning("*");
              if (!updatedTeam) throw new Error("No matching team to update!");
              teamToReturn = updatedTeam;
            } else if (team.id) {
              // true if we're only upating the escalationTags
              teamToReturn = team;
            } else {
              // Create new team
              const [newTeam] = await trx("team")
                .insert({
                  organization_id: organizationId,
                  author_id: user.id,
                  ...payload
                })
                .returning("*");

              teamToReturn = newTeam;
            }

            // Update team_escalation_tags
            if (team.escalationTagIds) {
              await trx("team_escalation_tags")
                .where({ team_id: teamToReturn.id })
                .del();

              teamToReturn.escalationTags = await trx("team_escalation_tags")
                .insert(
                  team.escalationTagIds.map(tagId => ({
                    team_id: teamToReturn.id,
                    tag_id: tagId
                  }))
                )
                .returning("*");
            }

            return teamToReturn;
          })
        );
      });

      await Promise.all([
        memoizer.invalidate(cacheOpts.OrganizationSingleTon.key, {
          organizationId
        }),
        memoizer.invalidate(cacheOpts.MyCurrentAssignmentTargets.key, {
          organizationId
        })
      ]);

      return updatedTeams;
    },
    deleteTeam: async (_, { organizationId, teamId }, { user }) => {
      await accessRequired(user, organizationId, "ADMIN");

      const deleteCount = await r
        .knex("team")
        .where({
          id: teamId,
          organization_id: organizationId
        })
        .del();
      if (deleteCount !== 1) throw new Error("Could not delete the team.");

      return true;
    },
    addUsersToTeam: async (_, { teamId, userIds }, { user }) => {
      const { organization_id } = await r
        .knex("team")
        .where({ id: teamId })
        .first("organization_id");
      await accessRequired(user, organization_id, "ADMIN");
      const userOrgCount = await r.parseCount(
        r
          .knex("user_organization")
          .where({ organization_id })
          .whereIn("user_id", userIds)
          .count()
      );
      if (userOrgCount !== userIds.length)
        throw new Error(
          "Tried adding user to team in organization they are not part of!"
        );
      const payload = userIds.map(userId => ({
        user_id: userId,
        team_id: teamId
      }));
      // This will throw for duplicate memberships. That is fine.
      await r.knex("user_team").insert(payload);
      return true;
    },
    removeUsersFromTeam: async (_, { teamId, userIds }, { user }) => {
      const { organization_id } = await r
        .knex("team")
        .where({ id: teamId })
        .first("organization_id");
      await accessRequired(user, organization_id, "ADMIN");
      const result = await r
        .knex("user_team")
        .where({ team_id: teamId })
        .whereIn("user_id", userIds)
        .del();
      return true;
    },
    releaseMyReplies: async (_, { organizationId }, { user }) => {
      await accessRequired(user, organizationId, "TEXTER");

      await r.knex.raw(
        `
        update campaign_contact
        set assignment_id = null
        from assignment
        where assignment_id = assignment.id
          and assignment.user_id = ?
          and message_status = 'needsResponse'
          and archived = false
      `,
        [user.id]
      );

      return true;
    },
    dismissMatchingAlarms: async (_, { token, organizationId }, { user }) => {
      await accessRequired(user, organizationId, "SUPERVOLUNTEER");
      await r
        .knex("troll_alarm")
        .update({ dismissed: true })
        .where({
          dismissed: false,
          trigger_token: token
        })
        .whereExists(function() {
          this.select(r.knex.raw("1"))
            .from("message")
            .join(
              "campaign_contact",
              "campaign_contact.id",
              "message.campaign_contact_id"
            )
            .join("campaign", "campaign.id", "campaign_contact.campaign_id")
            .where({ organization_id: organizationId })
            .whereRaw("message.id = troll_alarm.message_id");
        });

      return true;
    },
    dismissAlarms: async (_, { messageIds, organizationId }, { user }) => {
      await accessRequired(user, organizationId, "SUPERVOLUNTEER");
      await r
        .knex("troll_alarm")
        .update({ dismissed: true })
        .whereIn("message_id", messageIds);

      return true;
    },
    addToken: async (_, { token, organizationId }, { user }) => {
      await accessRequired(user, organizationId, "SUPERVOLUNTEER");
      await r
        .knex("troll_trigger")
        .insert({ token, organization_id: parseInt(organizationId) });

      return true;
    },
    removeToken: async (_, { token, organizationId }, { user }) => {
      await accessRequired(user, organizationId, "SUPERVOLUNTEER");
      await r
        .knex("troll_trigger")
        .where({ token, organization_id: parseInt(organizationId) })
        .del();

      return true;
    },
    createExternalSystem: async (
      _,
      { organizationId, externalSystem },
      { user }
    ) => {
      await accessRequired(user, organizationId, "ADMIN");

      const truncatedKey = externalSystem.apiKey.slice(0, 5) + "********";
      const apiKeyRef = graphileSecretRef(organizationId, truncatedKey);

      await getWorker().then(worker =>
        worker.setSecret(apiKeyRef, externalSystem.apiKey)
      );

      const [created] = await r
        .knex("external_system")
        .insert({
          name: externalSystem.name,
          type: externalSystem.type.toLowerCase(),
          organization_id: parseInt(organizationId),
          username: externalSystem.username,
          api_key_ref: apiKeyRef
        })
        .returning("*");

      // Kick off initial list load
      await r.knex.raw("select * from public.queue_refresh_saved_lists(?)", [
        created.id
      ]);

      return created;
    },
    editExternalSystem: async (
      _,
      { id: externalSystemId, externalSystem },
      { user }
    ) => {
      const savedSystem = await r
        .knex("external_system")
        .where({ id: externalSystemId })
        .first();

      await accessRequired(user, savedSystem.organization_id, "ADMIN");

      // We will check if the password/API key changed below
      let authDidChange = externalSystem.username !== savedSystem.username;

      const payload = {
        name: externalSystem.name,
        type: externalSystem.type.toLowerCase(),
        username: externalSystem.username
      };

      if (!externalSystem.apiKey.includes("*")) {
        authDidChange = true;
        const truncatedKey = externalSystem.apiKey.slice(0, 5) + "********";
        const apiKeyRef = graphileSecretRef(
          savedSystem.organization_id,
          truncatedKey
        );
        await r
          .knex("graphile_secrets.secrets")
          .where({ ref: savedSystem.api_key_ref })
          .del();
        await getWorker().then(worker =>
          worker.setSecret(apiKeyRef, externalSystem.apiKey)
        );
        payload.api_key_ref = apiKeyRef;
      }

      const [updated] = await r
        .knex("external_system")
        .update(payload)
        .where({ id: externalSystemId })
        .returning("*");

      // Completely refresh external lists after auth credentials change to make sure we're
      // not caching lists the new credentials do not have access to
      if (authDidChange) {
        await r.knex.raw("select * from public.queue_refresh_saved_lists(?)", [
          savedSystem.id
        ]);
      }

      return updated;
    },
    refreshExternalSystem: async (_, { externalSystemId }, { user }) => {
      const externalSystem = await r
        .knex("external_system")
        .where({ id: externalSystemId })
        .first();

      await accessRequired(user, externalSystem.organization_id, "ADMIN");

      await r.knex.raw("select * from public.queue_refresh_saved_lists(?)", [
        externalSystemId
      ]);

      return true;
    }
  }
};

const rootResolvers = {
  Action: {
    name: o => o.name,
    display_name: o => o.display_name,
    instructions: o => o.instructions
  },
  FoundContact: {
    found: o => o.found
  },
  RootQuery: {
    campaign: async (_, { id }, { loaders, user }) => {
      const campaign = await loaders.campaign.load(id);
      if (!campaign) {
        return null;
      }
      await accessRequired(user, campaign.organization_id, "SUPERVOLUNTEER");
      return campaign;
    },
    assignment: async (_, { id }, { loaders, user }) => {
      authRequired(user);
      const assignment = await loaders.assignment.load(id);
      const campaign = await loaders.campaign.load(assignment.campaign_id);
      if (assignment.user_id == user.id) {
        await accessRequired(
          user,
          campaign.organization_id,
          "TEXTER",
          /* allowSuperadmin=*/ true
        );
      } else {
        await accessRequired(
          user,
          campaign.organization_id,
          "SUPERVOLUNTEER",
          /* allowSuperadmin=*/ true
        );
      }
      return assignment;
    },
    organization: async (_, { id }, { loaders }) => {
      const getOrganization = memoizer.memoize(async ({ organizationId }) => {
        return await loaders.organization.load(id);
      }, cacheOpts.OrganizationSingleTon);

      return await getOrganization({ organizationId: id });
    },
    team: async (_, { id }, { user }) => {
      const team = await r
        .knex("team")
        .where({ id })
        .first();
      await accessRequired(user, team.organization_id, "SUPERVOLUNTEER");
      return team;
    },
    // TODO: this return a single element, not a single element array
    inviteByHash: async (_, { hash }, { loaders, user }) => {
      authRequired(user);
      return r.reader("invite").where({ hash });
    },
    currentUser: async (_, { id }, { user }) => {
      if (!user) {
        return null;
      } else {
        return user;
      }
    },
    contact: async (_, { id }, { loaders, user }) => {
      authRequired(user);
      const contact = await loaders.campaignContact.load(id);
      const campaign = await loaders.campaign.load(contact.campaign_id);
      await accessRequired(
        user,
        campaign.organization_id,
        "TEXTER",
        /* allowSuperadmin=*/ true
      );
      return contact;
    },
    organizations: async (_, { id }, { user }) => {
      await superAdminRequired(user);
      return r.reader("organization");
    },
    availableActions: (_, { organizationId }, { user }) => {
      if (!config.ACTION_HANDLERS) {
        return [];
      }
      const allHandlers = config.ACTION_HANDLERS.split(",");

      const availableHandlers = allHandlers
        .map(handler => {
          return {
            name: handler,
            handler: require(`../action_handlers/${handler}.js`)
          };
        })
        .filter(async h => h && (await h.handler.available(organizationId)));

      const availableHandlerObjects = availableHandlers.map(handler => {
        return {
          name: handler.name,
          display_name: handler.handler.displayName(),
          instructions: handler.handler.instructions()
        };
      });
      return availableHandlerObjects;
    },
    conversations: async (
      _,
      {
        cursor,
        organizationId,
        campaignsFilter,
        assignmentsFilter,
        tagsFilter,
        contactsFilter,
        contactNameFilter
      },
      { user }
    ) => {
      await accessRequired(user, organizationId, "SUPERVOLUNTEER", true);

      return getConversations(
        cursor,
        organizationId,
        campaignsFilter,
        assignmentsFilter,
        tagsFilter,
        contactsFilter,
        contactNameFilter
      );
    },
    campaigns: async (
      _,
      { organizationId, cursor, campaignsFilter },
      { user }
    ) => {
      await accessRequired(user, organizationId, "SUPERVOLUNTEER");
      return getCampaigns(organizationId, cursor, campaignsFilter);
    },
    people: async (
      _,
      { organizationId, cursor, campaignsFilter, role },
      { user }
    ) => {
      await accessRequired(user, organizationId, "SUPERVOLUNTEER");
      return getUsers(organizationId, cursor, campaignsFilter, role);
    },
    peopleByUserIds: async (_, { organizationId, userIds }, { user }) => {
      await accessRequired(user, organizationId, "SUPERVOLUNTEER");
      return getUsersById(userIds);
    },
    fetchCampaignOverlaps: async (
      _,
      { organizationId, campaignId },
      { user }
    ) => {
      await accessRequired(user, organizationId, "ADMIN");

      const { rows } = await queryCampaignOverlaps(campaignId, organizationId);

      const toReturn = rows.map(
        ({ campaign_id, count, campaign_title, last_activity }) => ({
          campaign: { id: campaign_id, title: campaign_title },
          overlapCount: count,
          lastActivity: last_activity
        })
      );

      return toReturn;
    },
    assignmentRequests: async (_, { organizationId, status }, { user }) => {
      await accessRequired(user, organizationId, "SUPERVOLUNTEER");

      const query = r
        .knex("assignment_request")
        .select(
          "assignment_request.*",
          "user.id as user_id",
          "user.first_name",
          "user.last_name"
        )
        .join("user", "user_id", "=", "user.id")
        .where({
          organization_id: organizationId
        });

      if (status) {
        query.where({ status });
      }

      const assignmentRequests = await query;
      const result = assignmentRequests.map(ar => {
        ar.user = {
          id: ar.user_id,
          first_name: ar.first_name,
          last_name: ar.last_name
        };
        ar.organization = { id: ar.organization_id };
        return ar;
      });
      return result;
    },
    trollAlarms: async (
      _,
      { limit, offset, token, dismissed, organizationId },
      { user }
    ) => {
      organizationId = parseInt(organizationId);
      await accessRequired(user, organizationId, "SUPERVOLUNTEER");

      let query = r
        .reader("troll_alarm")
        .join("message", "message.id", "=", "troll_alarm.message_id")
        .join(
          "campaign_contact",
          "campaign_contact.id",
          "=",
          "message.campaign_contact_id"
        )
        .join("campaign", "campaign.id", "=", "campaign_contact.campaign_id")
        .where({ dismissed, organization_id: organizationId });

      if (token !== null) {
        query = query.where({ trigger_token: token });
      }

      const countQuery = query.clone();
      const [{ count: totalCount }] = await countQuery.count();
      const alarms = await query
        .join("user", "user.id", "message.user_id")
        .select(
          "message_id",
          "trigger_token as token",
          "dismissed",
          "message.text as message_text",
          "user.id",
          "user.first_name",
          "user.last_name",
          "user.email"
        )
        .orderBy("troll_alarm.message_id")
        .limit(limit)
        .offset(offset)
        .map(({ message_id, token, dismissed, message_text, ...user }) => ({
          message_id,
          token,
          dismissed,
          message_text,
          user
        }));

      return { alarms, totalCount };
    },
    trollTokens: async (_, { organizationId }, { user }) => {
      await accessRequired(user, organizationId, "SUPERVOLUNTEER");

      const tokens = await r
        .reader("troll_trigger")
        .where({ organization_id: parseInt(organizationId) });

      return tokens.map(t => ({
        id: t.token,
        token: t.token,
        organizationId
      }));
    },
    externalSystems: async (_, { organizationId, after, first }, { user }) => {
      await accessRequired(user, organizationId, "ADMIN");

      const query = r
        .reader("external_system")
        .where({ organization_id: parseInt(organizationId) });
      return await formatPage(query, { after, first });
    },
    externalLists: async (
      _,
      { organizationId, systemId, after, first },
      { user }
    ) => {
      await accessRequired(user, organizationId, "ADMIN");

      const query = r.reader("external_list").where({
        organization_id: parseInt(organizationId),
        system_id: systemId
      });
      return await formatPage(query, { after, first });
    }
  }
};

export const resolvers = {
  ...tagResolvers,
  ...teamResolvers,
  ...assignmentRequestResolvers,
  ...rootResolvers,
  ...userResolvers,
  ...membershipSchema,
  ...settingsSchema,
  ...organizationResolvers,
  ...campaignResolvers,
  ...assignmentResolvers,
  ...interactionStepResolvers,
  ...optOutResolvers,
  ...messageResolvers,
  ...campaignContactResolvers,
  ...cannedResponseResolvers,
  ...questionResponseResolvers,
  ...inviteResolvers,
  ...linkDomainResolvers,
  ...trollbotResolvers,
  ...externalListResolvers,
  ...externalSystemResolvers,
  ...{ Date: GraphQLDate },
  ...{ JSON: GraphQLJSON },
  ...{ Phone: GraphQLPhone },
  ...questionResolvers,
  ...conversationsResolver,
  ...rootMutations
};
