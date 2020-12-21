import Knex from "knex";
import Papa from "papaparse";
import moment from "moment";
import sortBy from "lodash/sortBy";

import { JobRequestRecord } from "../api/types";
import { r } from "../../server/models";
import { sendEmail } from "../../server/mail";
import logger from "../../logger";
import { errToObj } from "../utils";
import { deleteJob } from "../../workers/jobs";
import { uploadToCloud } from "../../workers/exports/upload";
import { Task } from "pg-compose";

export interface ExportForVANOptions {
  requesterId: number;
  vanIdField: string;
  includeUnmessaged: boolean;
}

interface VanExportRow {
  campaign_contact_id: number;
  VAN_ID: string;
  cell: string;
  first_name: string;
  last_name: string;
  value: string;
  date: string;
}

const CHUNK_SIZE = 1000;
const FILTER_MESSAGED_FRAGMENT = `and exists ( select 1 from message where campaign_contact_id = cc.id)`;

export const exportForVan: Task = async (
  job: JobRequestRecord,
  _helpers: any
) => {
  const reader: Knex = r.reader;
  const payload: ExportForVANOptions = JSON.parse(job.payload);
  const { requesterId, includeUnmessaged, vanIdField } = payload;

  const { email } = await reader("user")
    .where({ id: requesterId })
    .first(["email"]);
  const { title } = await reader("campaign")
    .where({ id: job.campaign_id })
    .first(["title"]);

  const vanIdSelector =
    vanIdField === "external_id"
      ? "cc.external_id"
      : `(cc.custom_fields::json)->>'${vanIdField}'`;

  const fetchChunk = async (lastContactId: number) => {
    const { rows } = await reader.raw<{ rows: VanExportRow[] }>(
      `
        with campaign_contact_ids as (
          select
            id,
            cell,
            first_name,
            last_name,
            ${vanIdSelector} as VAN_ID,
            created_at
          from campaign_contact cc
          where
            campaign_id = ?
            and id > ?
            ${includeUnmessaged ? "" : FILTER_MESSAGED_FRAGMENT}
          order by id asc
          limit ?
        )
        select
           cc.VAN_ID,
           cc.id as campaign_contact_id,
           cc.cell,
           cc.first_name,
           cc.last_name,
           coalesce(result_values.value, 'canvassed, no response') as value,
           to_char(result_values.canvassed_at,'MM-DD-YYYY') as date
         from campaign_contact_ids cc
         left join (
           select
            question_response.campaign_contact_id,
            interaction_step.question || ': ' || question_response.value as value,
            question_response.created_at as canvassed_at
           from question_response
           join interaction_step on 
             question_response.interaction_step_id = interaction_step.id
           union
           select
            campaign_contact_id,
            title as value,
            cct.created_at as canvassed_at
           from campaign_contact_tag cct
           join tag on cct.tag_id = tag.id
         ) result_values on result_values.campaign_contact_id = cc.id
         group by 1, 2, 3, 4, 5, 6, 7
         order by cc.id asc;
      `,
      [job.campaign_id, lastContactId, CHUNK_SIZE]
    );

    return rows;
  };

  let exportRows: Omit<VanExportRow, "campaign_contact_id">[] = [];
  let lastContactId = 0;
  do {
    const rows = await fetchChunk(lastContactId);
    exportRows = exportRows.concat(
      sortBy(rows.map(({ campaign_contact_id, ...rest }) => rest), ["cell"])
    );

    lastContactId =
      rows.length > 0 ? rows[rows.length - 1].campaign_contact_id : -1;
  } while (lastContactId > 0);

  const exportCsv = Papa.unparse(exportRows);

  const safeTitle = title.replace(/ /g, "_").replace(/\//g, "_");
  const timestamp = moment().format("YYYY-MM-DD-HH-mm-ss");
  const vanExpostKey = `${safeTitle}-${timestamp}.csv`;
  const exportUrl = await uploadToCloud(`${vanExpostKey}.csv`, exportCsv);

  await sendEmail({
    to: email,
    subject: `VAN export ready for ${title}`,
    text: `Your VAN exports is ready! This URL will be valid for 24 hours.\n\n${exportUrl}`
  }).catch((err: Error) => {
    logger.error("Error sending VAN export email", {
      ...errToObj(err),
      exportUrl
    });
  });

  await deleteJob(job.id);
};
