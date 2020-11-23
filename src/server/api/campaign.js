import { config } from "../../config";
import { sqlResolvers } from "./lib/utils";
import { formatPage } from "./lib/pagination";
import { r, cacheableData } from "../models";
import { currentEditors } from "../models/cacheable_queries";
import { getUsers } from "./user";
import { memoizer, cacheOpts } from "../memoredis";
import { accessRequired } from "./errors";
import { symmetricEncrypt } from "./lib/crypto";
import { emptyRelayPage } from "../../api/pagination";
import { ExternalSyncReadinessState } from "../../api/campaign";

export function addCampaignsFilterToQuery(queryParam, campaignsFilter) {
  let query = queryParam;

  if (campaignsFilter) {
    const resultSize = campaignsFilter.listSize ? campaignsFilter.listSize : 0;
    const pageSize = campaignsFilter.pageSize ? campaignsFilter.pageSize : 0;

    if ("isArchived" in campaignsFilter) {
      query = query.where("campaign.is_archived", campaignsFilter.isArchived);
    }
    if ("campaignId" in campaignsFilter) {
      query = query.where(
        "campaign.id",
        parseInt(campaignsFilter.campaignId, 10)
      );
    }
    if (resultSize && !pageSize) {
      query = query.limit(resultSize);
    }
    if (resultSize && pageSize) {
      query = query.limit(resultSize).offSet(pageSize);
    }
  }
  return query;
}

export function buildCampaignQuery(
  queryParam,
  organizationId,
  campaignsFilter,
  addFromClause = true
) {
  let query = queryParam;

  if (addFromClause) {
    query = query.from("campaign");
  }

  query = query.where("campaign.organization_id", organizationId);
  query = addCampaignsFilterToQuery(query, campaignsFilter);

  return query;
}

const doGetCampaigns = memoizer.memoize(
  async ({ organizationId, cursor, campaignsFilter }) => {
    let campaignsQuery = buildCampaignQuery(
      r.reader.select("*"),
      organizationId,
      campaignsFilter
    );
    campaignsQuery = campaignsQuery.orderBy("id", "asc");

    if (cursor) {
      // A limit of 0 means a page size of 'All'
      if (cursor.limit !== 0) {
        campaignsQuery = campaignsQuery
          .limit(cursor.limit)
          .offset(cursor.offset);
      }
      const campaigns = await campaignsQuery;

      const campaignsCountQuery = buildCampaignQuery(
        r.knex.count("*"),
        organizationId,
        campaignsFilter
      );

      const campaignsCount = await r.parseCount(campaignsCountQuery);

      const pageInfo = {
        limit: cursor.limit,
        offset: cursor.offset,
        total: campaignsCount
      };
      return {
        campaigns,
        pageInfo
      };
    } else {
      return await campaignsQuery;
    }
  },
  cacheOpts.CampaignsList
);

export async function getCampaigns(organizationId, cursor, campaignsFilter) {
  return await doGetCampaigns({ organizationId, cursor, campaignsFilter });
}

export const resolvers = {
  JobRequest: {
    ...sqlResolvers([
      "id",
      "assigned",
      "status",
      "jobType",
      "resultMessage",
      "createdAt",
      "updatedAt"
    ])
  },
  CampaignStats: {
    sentMessagesCount: async campaign => {
      const getSentMessagesCount = memoizer.memoize(async ({ campaignId }) => {
        return await r.parseCount(
          r
            .reader("campaign_contact")
            .join(
              "message",
              "message.campaign_contact_id",
              "campaign_contact.id"
            )
            .where({
              "campaign_contact.campaign_id": campaignId,
              "message.is_from_contact": false
            })
            .count()
        );
      }, cacheOpts.CampaignSentMessagesCount);

      return await getSentMessagesCount({ campaignId: campaign.id });
    },
    receivedMessagesCount: async campaign => {
      const getReceivedMessagesCount = memoizer.memoize(
        async ({ campaignId }) => {
          return await r.parseCount(
            r
              .reader("campaign_contact")
              .join(
                "message",
                "message.campaign_contact_id",
                "campaign_contact.id"
              )
              .where({
                "campaign_contact.campaign_id": campaignId,
                "message.is_from_contact": true
              })
              .count()
          );
        },
        cacheOpts.CampaignReceivedMessagesCount
      );

      return await getReceivedMessagesCount({ campaignId: campaign.id });
    },
    optOutsCount: async campaign => {
      const getOptOutsCount = memoizer.memoize(
        async ({ campaignId, archived }) => {
          return await r.getCount(
            r
              .reader("campaign_contact")
              .where({
                is_opted_out: true,
                campaign_id: campaignId
              })
              .whereRaw(`archived = ${archived}`) // partial index friendly
          );
        },
        cacheOpts.CampaignOptOutsCount
      );

      return await getOptOutsCount({
        campaignId: campaign.id,
        archived: campaign.is_archived
      });
    }
  },
  CampaignReadiness: {
    id: ({ id }) => id,
    basics: campaign =>
      campaign.title !== "" &&
      campaign.description !== "" &&
      campaign.due_by !== null,
    textingHours: campaign =>
      campaign.textingHoursStart !== null &&
      campaign.textingHoursEnd !== null &&
      campaign.timezone !== null,
    integration: () => true,
    contacts: campaign =>
      r
        .reader("campaign_contact")
        .select("campaign_contact.id")
        .where({ campaign_id: campaign.id })
        .limit(1)
        .then(records => records.length > 0),
    autoassign: () => true,
    cannedResponses: () => true
  },
  CampaignsReturn: {
    __resolveType(obj, context, _) {
      if (Array.isArray(obj)) {
        return "CampaignsList";
      } else if ("campaigns" in obj && "pageInfo" in obj) {
        return "PaginatedCampaigns";
      }
      return null;
    }
  },
  CampaignsList: {
    campaigns: campaigns => {
      return campaigns;
    }
  },
  PaginatedCampaigns: {
    campaigns: queryResult => {
      return queryResult.campaigns;
    },
    pageInfo: queryResult => {
      if ("pageInfo" in queryResult) {
        return queryResult.pageInfo;
      }
      return null;
    }
  },
  Campaign: {
    ...sqlResolvers([
      "id",
      "title",
      "description",
      "isStarted",
      "isArchived",
      // TODO: re-enable once dynamic assignment is fixed (#548)
      // "useDynamicAssignment",
      "introHtml",
      "primaryColor",
      "logoImageUrl",
      "textingHoursStart",
      "textingHoursEnd",
      "isAutoassignEnabled",
      "timezone",
      "createdAt",
      "landlinesFiltered"
    ]),
    readiness: campaign => campaign,
    repliesStaleAfter: campaign => campaign.replies_stale_after_minutes,
    useDynamicAssignment: _ => false,
    isAssignmentLimitedToTeams: campaign => campaign.limit_assignment_to_teams,
    dueBy: campaign =>
      campaign.due_by instanceof Date || !campaign.due_by
        ? campaign.due_by || null
        : new Date(campaign.due_by),
    organization: async (campaign, _, { loaders }) =>
      campaign.organization ||
      loaders.organization.load(campaign.organization_id),
    datawarehouseAvailable: (campaign, _, { user }) =>
      user.is_superadmin && config.WAREHOUSE_DB_TYPE !== undefined,
    pendingJobs: async (campaign, { jobTypes = [] }) => {
      const query = r
        .reader("job_request")
        .where({ campaign_id: campaign.id })
        .orderBy("updated_at", "desc");
      if (jobTypes.length > 0) {
        query.whereIn("job_type", jobTypes);
      }
      return query;
    },
    teams: async campaign => {
      const getCampaignTeams = memoizer.memoize(async ({ campaignId }) => {
        return await r
          .reader("team")
          .select("team.*")
          .join("campaign_team", "campaign_team.team_id", "=", "team.id")
          .where({
            "campaign_team.campaign_id": campaign.id
          });
      }, cacheOpts.CampaignTeams);

      return await getCampaignTeams({ campaignId: campaign.id });
    },
    texters: async campaign =>
      getUsers(campaign.organization_id, null, { campaignId: campaign.id }),
    assignments: async (campaign, { assignmentsFilter = {} }) => {
      // TODO: permissions check needed
      let query = r.reader("assignment").where({ campaign_id: campaign.id });

      if (assignmentsFilter.texterId) {
        query = query.where({ user_id: assignmentsFilter.texterId });
      }

      return query;
    },
    interactionSteps: async campaign => {
      if (campaign.interactionSteps) {
        return campaign.interactionSteps;
      }

      const getInteractionSteps = memoizer.memoize(async ({ campaignId }) => {
        const interactionSteps = await cacheableData.campaign.dbInteractionSteps(
          campaignId
        );
        return interactionSteps;
      }, cacheOpts.CampaignInteractionSteps);

      return await getInteractionSteps({ campaignId: campaign.id });
    },
    cannedResponses: async (campaign, { userId }) => {
      const getCannedResponses = memoizer.memoize(
        async ({ campaignId, userId }) => {
          return await cacheableData.cannedResponse.query({
            userId: userId || "",
            campaignId: campaignId
          });
        },
        cacheOpts.CampaignCannedResponses
      );

      return await getCannedResponses({ campaignId: campaign.id });
    },
    contacts: async campaign =>
      r
        .reader("campaign_contact")
        .where({ campaign_id: campaign.id })
        .whereRaw(`archived = ${campaign.is_archived}`), // partial index friendly
    contactsCount: async campaign =>
      await r.getCount(
        r
          .reader("campaign_contact")
          .where({ campaign_id: campaign.id })
          .whereRaw(`archived = ${campaign.is_archived}`) // partial index friendly
      ),
    hasUnassignedContacts: async campaign => {
      if (config.BAD_BENS_DISABLE_HAS_UNASSIGNED_CONTACTS) {
        return false;
      }

      if (
        config.HIDE_CAMPAIGN_STATE_VARS_ON_ARCHIVED_CAMPAIGNS &&
        campaign.is_archived
      ) {
        return false;
      }

      const getHasUnassignedContacts = memoizer.memoize(
        async ({ campaignId, archived }) => {
          // SQL injection for archived = to enable use of partial index
          const { rows } = await r.reader.raw(
            `
            select exists (
              select 1
              from campaign_contact
              where
                campaign_id = ?
                and assignment_id is null
                and archived = ${archived}
                and not exists (
                  select 1
                  from campaign_contact_tag
                  join tag on campaign_contact_tag.tag_id = tag.id
                  where tag.is_assignable = false
                    and campaign_contact_tag.campaign_contact_id = campaign_contact.id
                )
                and is_opted_out = false
            ) as contact_exists
          `,
            [campaignId]
          );

          return rows[0] && rows[0].contact_exists;
        },
        cacheOpts.CampaignHasUnassignedContacts
      );

      return await getHasUnassignedContacts({
        campaignId: campaign.id,
        archived: campaign.is_archived
      });
    },
    hasUnsentInitialMessages: async campaign => {
      if (
        config.HIDE_CAMPAIGN_STATE_VARS_ON_ARCHIVED_CAMPAIGNS &&
        campaign.is_archived
      ) {
        return false;
      }

      const getHasUnsentInitialMessages = memoizer.memoize(
        async ({ campaignId, archived }) => {
          const contacts = await r
            .reader("campaign_contact")
            .select("id")
            .where({
              campaign_id: campaignId,
              message_status: "needsMessage",
              is_opted_out: false
            })
            .whereRaw(`archived = ${archived}`) // partial index friendly
            .limit(1);
          return contacts.length > 0;
        },
        cacheOpts.CampaignHasUnsentInitialMessages
      );

      return await getHasUnsentInitialMessages({
        campaignId: campaign.id,
        archived: campaign.is_archived
      });
    },
    hasUnhandledMessages: async campaign => {
      if (
        config.HIDE_CAMPAIGN_STATE_VARS_ON_ARCHIVED_CAMPAIGNS &&
        campaign.is_archived
      ) {
        return false;
      }

      const getHasUnhandledMessages = memoizer.memoize(
        async ({ campaignId, archived, organizationId }) => {
          let contactsQuery = r
            .reader("campaign_contact")
            .pluck("campaign_contact.id")
            .where({
              "campaign_contact.campaign_id": campaignId,
              message_status: "needsResponse",
              is_opted_out: false
            })
            .whereRaw(`archived = ${archived}`) // partial index friendly
            .limit(1);

          const notAssignableTagSubQuery = r.reader
            .select("campaign_contact_tag.campaign_contact_id")
            .from("campaign_contact_tag")
            .join("tag", "tag.id", "=", "campaign_contact_tag.tag_id")
            .where({
              "tag.organization_id": organizationId
            })
            .whereRaw("lower(tag.title) = 'escalated'")
            .whereRaw(
              "campaign_contact_tag.campaign_contact_id = campaign_contact.id"
            );

          contactsQuery = contactsQuery.whereNotExists(
            notAssignableTagSubQuery
          );

          const contacts = await contactsQuery;
          return contacts.length > 0;
        },
        cacheOpts.CampaignHasUnhandledMessages
      );

      return await getHasUnhandledMessages({
        campaignId: campaign.id,
        archived: campaign.is_archived,
        organizationId: campaign.organization_id
      });
    },
    customFields: async campaign =>
      campaign.customFields ||
      cacheableData.campaign.dbCustomFields(campaign.id),
    stats: async campaign => campaign,
    editors: async (campaign, _, { user }) => {
      if (r.redis) {
        return currentEditors(r.redis, campaign, user);
      }
      return "";
    },
    creator: async (campaign, _, { loaders }) =>
      campaign.creator_id ? loaders.user.load(campaign.creator_id) : null,
    previewUrl: async (campaign, _, { user }) => {
      const organizaitonId = await getCampaignOrganization({
        campaignId: campaign.id
      });
      await accessRequired(user, organizaitonId, "ADMIN");
      const token = symmetricEncrypt(`${campaign.id}`);
      return token;
    },
    externalSystem: async campaign =>
      campaign.external_system_id
        ? r
            .reader("external_system")
            .where({ id: campaign.external_system_id })
            .first()
        : null,
    syncReadiness: async campaign => {
      if (!campaign.external_system_id)
        return ExternalSyncReadinessState.MISSING_SYSTEM;

      const {
        rows: [{ missing_and_required, includes_not_active }]
      } = await r.reader.raw(
        `
          select
            count(*) filter (where is_missing and is_required) as missing_and_required,
            count(*) filter (where includes_not_active) as includes_not_active
          from public.external_sync_question_response_configuration
          where
            campaign_id = ?
            and system_id = ?
        `,
        [campaign.id, campaign.external_system_id]
      );

      return missing_and_required > 0
        ? ExternalSyncReadinessState.MISSING_REQUIRED_MAPPING
        : includes_not_active > 0
          ? ExternalSyncReadinessState.INCLUDES_NOT_ACTIVE_TARGETS
          : ExternalSyncReadinessState.READY;
    },
    externalSyncConfigurations: async (campaign, { after, first }) => {
      if (!campaign.external_system_id) return emptyRelayPage();

      const query = r
        .reader("external_sync_question_response_configuration")
        .where({
          campaign_id: campaign.id,
          system_id: campaign.external_system_id
        });
      return formatPage(query, { after, first, primaryColumn: "compound_id" });
    }
  }
};

const getCampaignOrganization = memoizer.memoize(async ({ campaignId }) => {
  const campaign = await r
    .reader("campaign")
    .where({ id: campaignId })
    .first("organization_id");
  return campaign.organization_id;
}, cacheOpts.CampaignOrganizationId);
