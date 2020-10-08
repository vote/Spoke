import iconv from "iconv-lite";
import AutoDetectDecoderStream from "autodetect-decoder-stream";
import partition from "lodash/partition";
import Papa from "papaparse";
import { URL } from "url";

import {
  validateCsv,
  requiredUploadFields,
  topLevelUploadFields,
  fieldAliases
} from "../../../lib";

const missingHeaderFields = fields =>
  requiredUploadFields.reduce((missingFields, requiredField) => {
    return fields.includes(requiredField)
      ? missingFields
      : missingFields.concat([requiredField]);
  }, []);

const isTopLevelEntry = ([field, _]) => topLevelUploadFields.includes(field);
const trimEntry = ([key, value]) => [key, value.trim()];
const FIELD_DEFAULTS = { external_id: "", zip: "" };

const sanitizeRawContact = rawContact => {
  const allFields = Object.entries({ ...FIELD_DEFAULTS, ...rawContact });
  const [contactEntries, customFieldEntries] = partition(
    allFields,
    isTopLevelEntry
  );
  const contact = Object.fromEntries(contactEntries.map(trimEntry));
  const customFields = Object.fromEntries(customFieldEntries.map(trimEntry));
  return { ...contact, customFields };
};

const findInvalidHrefFields = contact => {
  return Object.keys(contact.customFields)
    .filter(key => key.startsWith("href_"))
    .filter(key => {
      try {
        // eslint-disable-next-line no-new
        new URL(contact.customFields[key]);
        return false;
      } catch (e) {
        return true;
      }
    });
};

export const processContactsFile = async file => {
  const { createReadStream } = await file;
  const stream = createReadStream()
    .pipe(new AutoDetectDecoderStream())
    .pipe(iconv.encodeStream("utf8"));

  return new Promise((resolve, reject) => {
    let abortMessage;
    let resultMeta = undefined;
    const resultData = [];

    Papa.parse(stream, {
      header: true,
      transformHeader: header => {
        for (const [field, aliases] of Object.entries(fieldAliases)) {
          if (aliases.includes(header)) {
            return field;
          }
        }
        return header;
      },
      step: ({ data, meta, errors }, parser) => {
        // Exit early on bad header
        if (resultMeta === undefined) {
          resultMeta = meta;
          const missingFields = missingHeaderFields(meta.fields);
          abortMessage = `CSV missing fields: ${missingFields}`;
          if (missingFields.length > 0) {
            parser.abort();
          }
        }
        const contact = sanitizeRawContact(data);

        // exit early if any href fields contain invalid urls
        const invalidHrefs = findInvalidHrefFields(contact);
        if (invalidHrefs.length > 0) {
          abortMessage = `CSV contains invalid hrefs: ${invalidHrefs}`;
          parser.abort();
        }
        
        resultData.push(contact);
      },
      complete: ({ meta: { aborted } }) => {
        if (aborted) return reject(abortMessage);
        const { contacts, validationStats } = validateCsv({
          data: resultData,
          meta: resultMeta
        });
        return resolve({ contacts, validationStats });
      },
      error: err => reject(err)
    });
  });
};
