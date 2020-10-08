import NumbersClient from "assemble-numbers-client";
import { chunk } from "lodash";
import { Pool } from "pg";
import { messageComponents } from "../src/server/api/lib/message-sending";

/* tslint:disable */

const pool = new Pool({ connectionString: process.env.SPOKE_DATABASE_URL });

const start = new Date();
const summary = async () => {
  console.log("END", (new Date() - start) / 1000);
  const { rows: result } = await pool.query(`
    SELECT COUNT(*)
    from message
             join campaign_contact on campaign_contact.id = message.campaign_contact_id
             join campaign on campaign_contact.campaign_id = campaign.id
             join messaging_service_stick on messaging_service_stick.cell = message.contact_number
        and messaging_service_stick.organization_id = campaign.organization_id
             JOIN messaging_service
                  ON messaging_service.messaging_service_sid = messaging_service_stick.messaging_service_sid
    WHERE message.created_at >= '2020-10-03T12:00:00Z'
      AND message.created_at <= '2020-10-05T16:00:00Z'
      AND message.send_status = 'ERROR'
      AND NOT message.is_from_contact
      AND NOT campaign_contact.is_opted_out
      AND service_id = ''
      AND messaging_service.account_sid IN (
                                            'https://switchboard2.voteamericaspoke.com',
                                            'https://switchboard3.voteamericaspoke.com',
                                            'https://switchboard4.voteamericaspoke.com',
                                            'https://switchboard5.voteamericaspoke.com',
                                            'https://switchboard6.voteamericaspoke.com',
                                            'https://switchboard7.voteamericaspoke.com',
                                            'https://switchboard8.voteamericaspoke.com',
                                            'https://switchboard9.voteamericaspoke.com'
        )
      AND campaign_id NOT IN (255, 256, 257, 258, 259)
  `);

  return result;
};

const BATCH_SIZE = 1500000;
const CONCURRENCY = 100;

const resendMessage = async (
  id,
  text,
  contactNumber,
  profileId,
  contactZipCode,
  accountSid
) => {
  try {
    console.log(`SENDING [${id}] [${accountSid}]: contact number ${contactNumber}`);
    const { body, mediaUrl } = messageComponents(text);
    const mediaUrls = mediaUrl ? [mediaUrl] : undefined;
    const messageInput = {
      profileId,
      to: contactNumber,
      body,
      mediaUrls,
      contactZipCode: contactZipCode === "" ? null : contactZipCode
    };

    const numbersClient = new NumbersClient({
      apiKey: process.env.SWITCHBOARD_API_KEY,
      endpointBaseUrl: accountSid
    });

    const sent = await numbersClient.sms.sendMessage(messageInput);
    console.log(`RESPONSE [${id}] [${accountSid}]: ${JSON.stringify(sent)}`);

    const serviceId = sent.data.sendMessage.outboundMessage.id;

    await pool.query(
      "update message set send_status = $1, service_id = $2 where id = $3",
      ["SENT", serviceId, id]
    );
  } catch (ex) {
    console.log(`ERROR [${id}] [${accountSid}]`, ex);
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
      campaign_contact.zip,
      messaging_service.account_sid
    from message
    join campaign_contact on campaign_contact.id = message.campaign_contact_id
    join campaign on campaign_contact.campaign_id = campaign.id
    join messaging_service_stick on messaging_service_stick.cell = message.contact_number
      and messaging_service_stick.organization_id = campaign.organization_id
    JOIN messaging_service
         ON messaging_service.messaging_service_sid = messaging_service_stick.messaging_service_sid
    WHERE message.created_at >= '2020-10-03T12:00:00Z'
      AND message.created_at <= '2020-10-05T16:00:00Z'
      AND message.send_status = 'ERROR'
      AND NOT message.is_from_contact
      AND NOT campaign_contact.is_opted_out
      AND service_id = ''
      AND messaging_service.account_sid IN (
          'https://switchboard2.voteamericaspoke.com',
          'https://switchboard3.voteamericaspoke.com',
          'https://switchboard4.voteamericaspoke.com',
          'https://switchboard5.voteamericaspoke.com',
          'https://switchboard6.voteamericaspoke.com',
          'https://switchboard7.voteamericaspoke.com',
          'https://switchboard8.voteamericaspoke.com',
          'https://switchboard9.voteamericaspoke.com'
      )
     AND campaign_id NOT IN (255, 256, 257, 258, 259)
    ORDER BY message.created_at ASC
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
          message.zip,
          message.account_sid
        )
      )
    );

    batchCount++;
  }

  // for each
  // send each message using numbers-client
  // update service id and send status
};

summary()
  .then(() => rerun("America/New_York"))
  .then(summary)
  .then(console.log);
