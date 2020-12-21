import { RequestAutoApproveType } from "./organization-membership";

export interface OrganizationSettingsInput {
  defaulTexterApprovalStatus: RequestAutoApproveType | null;
  optOutMessage: string | null;
  numbersApiKey: string | null;
  trollbotWebhookUrl: string | null;
  showContactLastName: boolean | null;
  showContactCell: boolean | null;
}

export interface OranizationSettings {
  id: string;
  defaulTexterApprovalStatus: RequestAutoApproveType;
  optOutMessage: string | null;
  numbersApiKey: string | null;
  trollbotWebhookUrl: string | null;
  showContactLastName: boolean | null;
  showContactCell: boolean | null;
}

export const schema = `
  input OrganizationSettingsInput {
    defaulTexterApprovalStatus: RequestAutoApprove
    optOutMessage: String
    numbersApiKey: String
    trollbotWebhookUrl: String
    showContactLastName: Boolean
    showContactCell: Boolean
  }

  type OranizationSettings {
    id: ID!
    defaulTexterApprovalStatus: RequestAutoApprove!
    optOutMessage: String
    numbersApiKey: String
    trollbotWebhookUrl: String
    showContactLastName: Boolean
    showContactCell: Boolean
  }
`;
