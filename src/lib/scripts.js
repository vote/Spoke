import escapeRegExp from "lodash/escapeRegExp";

export const delimiters = {
  startDelimiter: "{",
  endDelimiter: "}"
};

export const delimit = text => {
  const { startDelimiter, endDelimiter } = delimiters;
  return `${startDelimiter}${text}${endDelimiter}`;
};

// const REQUIRED_UPLOAD_FIELDS = ['firstName', 'lastName', 'cell']
const TOP_LEVEL_UPLOAD_FIELDS = [
  "firstName",
  "lastName",
  "cell",
  "zip",
  "external_id"
];
const TEXTER_SCRIPT_FIELDS = ["texterFirstName", "texterLastName"];

// Fields that should be capitalized when a script is applied
const CAPITALIZE_FIELDS = [
  "firstName",
  "lastName",
  "texterFirstName",
  "texterLastName"
];

// Special first names that should not be capitalized
const LOWERCASE_FIRST_NAMES = ["friend", "there"];

// TODO: This will include zipCode even if you ddin't upload it
export const allScriptFields = customFields =>
  TOP_LEVEL_UPLOAD_FIELDS.concat(TEXTER_SCRIPT_FIELDS).concat(customFields);

const capitalize = str => {
  const strTrimmed = str.trim();
  return strTrimmed.charAt(0).toUpperCase() + strTrimmed.slice(1).toLowerCase();
};

const getScriptFieldValue = (contact, texter, fieldName) => {
  let result;
  if (fieldName === "texterFirstName") {
    result = texter.firstName;
  } else if (fieldName === "texterLastName") {
    result = texter.lastName;
  } else if (TOP_LEVEL_UPLOAD_FIELDS.indexOf(fieldName) !== -1) {
    result = contact[fieldName];
  } else if (fieldName.startsWith("href_")) {
    const customFields = JSON.parse(contact.customFields);
    const href = new URL(customFields[fieldName]);
    href.searchParams.append("c", contact.id);
    result = href.toString();
  } else {
    const customFieldNames = JSON.parse(contact.customFields);
    result = customFieldNames[fieldName];
  }

  const isCapitalizedField = CAPITALIZE_FIELDS.includes(fieldName);
  const isSpecialFirstName =
    fieldName === "firstName" &&
    LOWERCASE_FIRST_NAMES.includes(result.toLowerCase());
  if (isCapitalizedField && !isSpecialFirstName) {
    result = capitalize(result);
  }

  return result;
};

export const applyScript = ({ script, contact, customFields, texter }) => {
  const scriptFields = allScriptFields(customFields);
  let appliedScript = script;

  for (const field of scriptFields) {
    const re = new RegExp(escapeRegExp(delimit(field)), "g");
    appliedScript = appliedScript.replace(
      re,
      getScriptFieldValue(contact, texter, field)
    );
  }
  return appliedScript;
};
