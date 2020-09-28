import NumbersClient from "assemble-numbers-client";
import { chunk } from "lodash";
import { Pool } from "pg";
import { messageComponents } from "../src/server/api/lib/message-sending";

/* tslint:disable */

const pool = new Pool({ connectionString: process.env.SPOKE_DATABASE_URL });
const numbersClient = new NumbersClient({
  apiKey: process.env.SWITCHBOARD_API_KEY,
  endpointBaseUrl: process.env.SWITCHBOARD_BASE_URL
});

const start = new Date()
const summary = async () => {
  console.log("END", (new Date() - start) / 1000);
  const { rows: result } = await pool.query(`
    SELECT COUNT(*)
    FROM public.message
    INNER JOIN ad_hoc.failed_messages_0923 fm
      ON (message.contact_number = fm.contact_number)
    WHERE message.created_at >= '2020-09-23T00:00:00Z'
      AND message.created_at < '2020-09-24T02:00:00Z'
      AND message.send_status = 'SENT'
      AND NOT fm.retried
      AND message.num_media > 0
      AND NOT message.is_from_contact
  `);

  return result;
};

const BATCH_SIZE = 290000;
const CONCURRENCY = 10;

const resendMessage = async (
  id,
  text,
  contactNumber,
  profileId,
  contactZipCode
) => {
  try {
    console.log(`SENDING [${id}]: contact number ${contactNumber}`);
    const { body, mediaUrl } = messageComponents(text);
    const mediaUrls = mediaUrl ? [mediaUrl] : undefined;
    const messageInput = {
      profileId,
      to: contactNumber,
      body,
      mediaUrls,
      contactZipCode: contactZipCode === "" ? null : contactZipCode
    };

    await pool.query(
      "update ad_hoc.failed_messages_0923 set retried = true where contact_number = $1",
      [contactNumber]
    );

    const sent = await numbersClient.sms.sendMessage(messageInput);
    console.log(`RESPONSE [${id}]: ${JSON.stringify(sent)}`);

    const serviceId = sent.data.sendMessage.outboundMessage.id;

    await pool.query(
      "update message set send_status = $1, service_id = $2 where id = $3",
      ["SENT", serviceId, id]
    );
  } catch (ex) {
    console.log(`ERROR [${id}]`, ex);
    console.log(text);
  }
};

const rerun = async timezone => {
  console.log("START", new Date());
  const { rows: messages } = await pool.query(
    `
    select
      message.id,
      message.text,
      message.contact_number,
      messaging_service_stick.messaging_service_sid,
      campaign_contact.zip
    from message
    join campaign_contact on campaign_contact.id = message.campaign_contact_id
    join campaign on campaign_contact.campaign_id = campaign.id
    join messaging_service_stick on messaging_service_stick.cell = message.contact_number
      and messaging_service_stick.organization_id = campaign.organization_id
    INNER JOIN ad_hoc.failed_messages_0923 fm
      ON (message.contact_number = fm.contact_number)
    WHERE message.created_at >= '2020-09-23T00:00:00Z'
      AND message.created_at < '2020-09-24T02:00:00Z'
      AND message.send_status = 'SENT'
      AND NOT fm.retried
      AND message.num_media > 0
      AND NOT message.is_from_contact
      -- AND campaign.id IN (2, 43, 5, 49)
      AND NOT campaign_contact.is_opted_out
       -- and coalesce(campaign_contact.timezone, campaign.timezone) = $2
    order by campaign.id
    limit $1
  `,
    [BATCH_SIZE] // p, timezone]
  );
  const countToSend = messages.length;
  console.log("SENDING", countToSend);
  const batches = chunk(messages, CONCURRENCY);

  let batchCount = 0;

  for (const batch of batches) {
    console.log("ITERATION: ", batchCount * CONCURRENCY);

    await Promise.all(
      batch.map(message =>
        resendMessage(
          message.id,
          message.text,
          message.contact_number,
          message.messaging_service_sid,
          message.zip
        )
      )
    );

    batchCount++;
  }

  // for each
  // send each message using numbers-client
  // update service id and send status
};

rerun("America/New_York")
  .then(summary)
  .then(console.log);
