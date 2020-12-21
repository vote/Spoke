import React from "react";
import gql from "graphql-tag";
import { compose } from "recompose";
import { ApolloQueryResult } from "apollo-client";

import RaisedButton from "material-ui/RaisedButton";
import SelectField from "material-ui/SelectField";
import MenuItem from "material-ui/MenuItem";

import { loadData } from "../../../hoc/with-operations";
import CampaignFormSectionHeading from "../../components/CampaignFormSectionHeading";
import {
  asSection,
  FullComponentProps,
  RequiredComponentProps
} from "../../components/SectionWrapper";
import CSVForm from "./components/CSVForm";
import ExternalSystemsSource from "./components/ExternalSystemsSource";
import SelectExcludeCampaigns from "./components/SelectExcludeCampaigns";
import ContactsSqlForm from "./components/ContactsSqlForm";
import UploadResults from "./components/UploadResults";
import { RelayPaginatedResponse } from "../../../../api/pagination";

enum ContactSourceType {
  CSV = "CSV",
  ExternalSystem = "Integration",
  SQL = "SQL"
}

interface ContactSource {
  source: ContactSourceType;
  disabledReason?: string;
}

interface ContactsValues {
  contactSql: string | null;
  contactsFile: File | null;
  filterOutLandlines: boolean;
  excludeCampaignIds: string[];
}

interface ContactsCampaign {
  id: string;
  customFields: string[];
  contactsCount: number;
  externalSystem: { id: string } | null;
  datawarehouseAvailable: boolean;
}

interface ContactsOrganization {
  id: string;
  numbersApiKey: string;
  externalSystems: Pick<RelayPaginatedResponse<never>, "pageInfo">;
  campaigns: {
    campaigns: {
      id: string;
      title: string;
      createdAt: string;
    }[];
  };
}

interface ContactsHocProps {
  mutations: {
    editCampaign(payload: ContactsValues): ApolloQueryResult<any>;
  };
  campaignData: {
    campaign: ContactsCampaign;
  };
  organizationData: {
    organization: ContactsOrganization;
  };
}

interface ContactsInnerProps extends FullComponentProps, ContactsHocProps {}

interface ContactsState {
  // Pending changes
  selectedCampaignIds: string[];
  contactsSql: string | null;
  contactsFile: File | null;
  externalListId: string | null;
  filterOutLandlines: boolean;

  // UI
  source: ContactSourceType;
  isWorking: boolean;
}

class CampaignContactsForm extends React.Component<
  ContactsInnerProps,
  ContactsState
> {
  state: ContactsState = {
    // UI
    source: ContactSourceType.CSV,
    isWorking: false,

    // Pending changes payload
    selectedCampaignIds: [],
    contactsSql: null,
    contactsFile: null,
    externalListId: null,
    filterOutLandlines: false
  };

  constructor(props: ContactsInnerProps) {
    super(props);

    const options = this.getSourceOptions();
    this.state.source = options.find(op => !op.disabledReason)!.source;
  }

  handleOnChangeValidSql = (contactsSql: string | null) =>
    this.setState({ contactsSql });

  handleOnContactsFileChange = (contactsFile?: File) =>
    this.setState({ contactsFile: contactsFile || null });

  handleOnExternalListChange = (externalListId: string) =>
    this.setState({ externalListId });

  handleOnChangeExcludedCamapignIds = (selectedCampaignIds: string[]) =>
    this.setState({ selectedCampaignIds });

  handleOnSubmit = async () => {
    const {
      contactsSql,
      contactsFile,
      externalListId,
      filterOutLandlines,
      selectedCampaignIds
    } = this.state;

    this.setState({ isWorking: true });
    try {
      const campaignInput = {
        contactSql: contactsSql,
        contactsFile,
        externalListId,
        filterOutLandlines,
        excludeCampaignIds: selectedCampaignIds
      };
      const response = await this.props.mutations.editCampaign(campaignInput);
      if (response.errors) throw response.errors;
      // TODO: this.props.onComplete();
    } catch (err) {
      this.props.onError(err.message);
    } finally {
      this.setState({ isWorking: false });
    }
  };

  handleOnChangeSource = (
    _e: React.SyntheticEvent<{}>,
    _index: number,
    source: any
  ) => {
    if (source !== ContactSourceType.CSV) {
      this.setState({ contactsFile: null });
    }
    if (source !== ContactSourceType.ExternalSystem) {
      this.setState({ externalListId: null });
    }
    if (source !== ContactSourceType.SQL) {
      this.setState({ contactsSql: null });
    }
    this.setState({ source });
  };

  getSourceOptions = (): ContactSource[] => {
    const {
      datawarehouseAvailable,
      externalSystem
    } = this.props.campaignData.campaign;
    const { externalSystems } = this.props.organizationData.organization;
    const sourceOptions = [];

    // CSV
    const csvDisabled = externalSystem !== null;
    sourceOptions.push({
      source: ContactSourceType.CSV,
      disabled: csvDisabled,
      disabledReason: csvDisabled ? "using an integration" : undefined
    });

    // Integration
    if (externalSystems.pageInfo.totalCount > 0) {
      const disabled = externalSystem === null;
      sourceOptions.push({
        source: ContactSourceType.ExternalSystem,
        disabledReason: disabled
          ? "not configured for this campaign"
          : undefined
      });
    }

    // Data warehouse
    if (datawarehouseAvailable) {
      sourceOptions.push({ source: ContactSourceType.SQL, disabled: false });
    }

    return sourceOptions;
  };

  render() {
    const {
      source,
      isWorking,
      selectedCampaignIds,
      contactsSql,
      contactsFile,
      externalListId
    } = this.state;
    const {
      campaignId,
      campaignData,
      organizationData,
      isNew,
      pendingJob,
      saveLabel
    } = this.props;
    const {
      customFields,
      contactsCount,
      externalSystem
    } = campaignData.campaign;

    const {
      campaigns: { campaigns: allCampaigns }
    } = organizationData.organization;

    const allOtherCampaigns = allCampaigns.filter(({ id }) => id != campaignId);

    const isSaveDisabled =
      isWorking || (!isNew && !contactsFile && !contactsSql && !externalListId);
    const finalSaveLabel = isWorking ? "Working..." : saveLabel;

    const sourceOptions = this.getSourceOptions();

    return (
      <div>
        <CampaignFormSectionHeading title="Who are you contacting?" />
        <SelectExcludeCampaigns
          allOtherCampaigns={allOtherCampaigns}
          selectedCampaignIds={selectedCampaignIds}
          onChangeExcludedCamapignIds={this.handleOnChangeExcludedCamapignIds}
        />
        <SelectField
          floatingLabelText="Contact source"
          value={source}
          fullWidth={true}
          onChange={this.handleOnChangeSource}
        >
          {sourceOptions.map(({ source, disabledReason }) => (
            <MenuItem
              key={source}
              value={source}
              primaryText={
                disabledReason ? `${source} (${disabledReason})` : source
              }
              disabled={disabledReason !== undefined}
            />
          ))}
        </SelectField>
        <br />
        <br />
        {source === ContactSourceType.CSV && (
          <CSVForm
            contactsFile={contactsFile}
            onContactsFileChange={this.handleOnContactsFileChange}
          />
        )}
        {source === ContactSourceType.ExternalSystem &&
          externalSystem && (
            <ExternalSystemsSource
              systemId={externalSystem.id}
              selectedListId={externalListId}
              onChangeExternalList={this.handleOnExternalListChange}
            />
          )}
        {source === ContactSourceType.SQL && (
          <ContactsSqlForm
            style={{ marginTop: "20px" }}
            onChangeValidSql={this.handleOnChangeValidSql}
          />
        )}
        <UploadResults
          contactsCount={contactsCount}
          customFields={customFields}
          pendingJob={pendingJob}
        />
        <br />
        <RaisedButton
          label={finalSaveLabel}
          disabled={isSaveDisabled}
          onClick={this.handleOnSubmit}
        />
      </div>
    );
  }
}

const queries = {
  campaignData: {
    query: gql`
      query getCampaignContacts($campaignId: String!) {
        campaign(id: $campaignId) {
          id
          customFields
          contactsCount
          externalSystem {
            id
          }
          datawarehouseAvailable
        }
      }
    `,
    options: (ownProps: ContactsInnerProps) => ({
      variables: {
        campaignId: ownProps.campaignId
      }
    })
  },
  organizationData: {
    query: gql`
      query getOrganizationDataForEditContacts($organizationId: String!) {
        organization(id: $organizationId) {
          id
          numbersApiKey
          externalSystems {
            pageInfo {
              totalCount
            }
          }
          campaigns(cursor: { offset: 0, limit: 5000 }) {
            campaigns {
              id
              title
              createdAt
            }
          }
        }
      }
    `,
    options: (ownProps: ContactsInnerProps) => ({
      variables: {
        organizationId: ownProps.organizationId
      }
    })
  }
};

const mutations = {
  editCampaign: (ownProps: ContactsInnerProps) => (
    payload: ContactsValues
  ) => ({
    mutation: gql`
      mutation editCampaignContacts(
        $campaignId: String!
        $payload: CampaignInput!
      ) {
        editCampaign(id: $campaignId, campaign: $payload) {
          id
          customFields
          contactsCount
          datawarehouseAvailable
          readiness {
            id
            contacts
          }
        }
      }
    `,
    variables: {
      campaignId: ownProps.campaignId,
      payload
    }
  })
};

export default compose<ContactsInnerProps, RequiredComponentProps>(
  asSection({
    title: "Contacts",
    readinessName: "contacts",
    jobQueueNames: [
      "upload_contacts",
      "upload_contacts_sql",
      "load_external_list"
    ],
    expandAfterCampaignStarts: false,
    expandableBySuperVolunteers: false
  }),
  loadData({
    queries,
    mutations
  })
)(CampaignContactsForm);
