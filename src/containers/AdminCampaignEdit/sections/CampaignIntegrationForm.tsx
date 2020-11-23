import React from "react";
import gql from "graphql-tag";
import { compose } from "recompose";
import { ApolloQueryResult } from "apollo-client";
import isEmpty from "lodash/isEmpty";

import RaisedButton from "material-ui/RaisedButton";
import SelectField from "material-ui/SelectField";
import MenuItem from "material-ui/MenuItem";

import { loadData } from "../../hoc/with-operations";
import { QueryMap, MutationMap } from "../../../network/types";
import { RelayPaginatedResponse } from "../../../api/pagination";
import { ExternalSystem } from "../../../api/external-system";
import {
  GET_SYNC_CONFIGS,
  GET_SYNC_TARGETS
} from "../../../components/SyncConfigurationModal/queries";
import CampaignFormSectionHeading from "../components/CampaignFormSectionHeading";
import {
  asSection,
  FullComponentProps,
  RequiredComponentProps
} from "../components/SectionWrapper";

interface IntegrationValues {
  externalSystemId?: string | null;
}

interface HocProps {
  mutations: {
    editCampaign(payload: IntegrationValues): ApolloQueryResult<any>;
  };
  data: {
    campaign: {
      id: string;
      externalSystem: { id: string };
    };
  };
  externalSystems: {
    organization: {
      id: string;
      externalSystems: RelayPaginatedResponse<
        Pick<ExternalSystem, "id" | "name" | "type">
      >;
    };
  };
}

interface InnerProps extends FullComponentProps, HocProps {}

interface State {
  pendingChanges: IntegrationValues;
  isWorking: boolean;
}

class CampaignIntegrationForm extends React.Component<InnerProps, State> {
  state: State = {
    pendingChanges: {},
    isWorking: false
  };

  handleChange = (
    e: React.SyntheticEvent<{}>,
    index: number,
    value: string
  ) => {
    const { externalSystem } = this.props.data.campaign;
    const existingId = externalSystem ? externalSystem.id : null;
    const pendingChanges = {
      externalSystemId: value !== existingId ? value : undefined
    };
    this.setState({ pendingChanges });
  };

  handleSubmit = async () => {
    const { pendingChanges } = this.state;
    const { editCampaign } = this.props.mutations;

    this.setState({ isWorking: true });
    try {
      const response = await editCampaign(pendingChanges);
      if (response.errors) throw response.errors;
      this.setState({ pendingChanges: {} });
    } catch (err) {
      this.props.onError(err.message);
    } finally {
      this.setState({ isWorking: false });
    }
  };

  render() {
    const { pendingChanges, isWorking } = this.state;
    const {
      data: { campaign },
      externalSystems: { organization },
      isNew,
      saveLabel
    } = this.props;
    const systems = organization.externalSystems.edges.map(edge => edge.node);
    const externalSystemId =
      pendingChanges.externalSystemId !== undefined
        ? pendingChanges.externalSystemId
        : (campaign.externalSystem || { id: null }).id;

    const hasPendingChanges = !isEmpty(pendingChanges);
    const isSaveDisabled = isWorking || (!isNew && !hasPendingChanges);

    const finalSaveLabel = isWorking ? "Working..." : saveLabel;

    return (
      <div>
        <CampaignFormSectionHeading
          title="Integration selection"
          subtitle="Selecting an integration for a campaign allows loading contacts into Spoke and exporting data out of Spoke."
        />
        <SelectField
          floatingLabelText="Integration"
          value={externalSystemId}
          style={{ width: "100%" }}
          onChange={this.handleChange}
        >
          <MenuItem value={null} primaryText="" />
          {systems.map(system => (
            <MenuItem
              key={system.id}
              value={system.id}
              primaryText={system.name}
              secondaryText={system.type}
            />
          ))}
        </SelectField>
        <br />
        <RaisedButton
          label={finalSaveLabel}
          disabled={isSaveDisabled}
          onClick={this.handleSubmit}
        />
      </div>
    );
  }
}

const queries: QueryMap<InnerProps> = {
  data: {
    query: gql`
      query getCampaignExternalSystem($campaignId: String!) {
        campaign(id: $campaignId) {
          id
          externalSystem {
            id
          }
          isStarted
        }
      }
    `,
    options: ownProps => ({
      variables: {
        campaignId: ownProps.campaignId
      }
    })
  },
  externalSystems: {
    query: gql`
      query getExternalSystems($organizationId: String!) {
        organization(id: $organizationId) {
          id
          externalSystems {
            edges {
              node {
                id
                name
                type
              }
            }
          }
        }
      }
    `,
    options: ownProps => ({
      variables: {
        organizationId: ownProps.organizationId
      }
    })
  }
};

const mutations: MutationMap<InnerProps> = {
  editCampaign: ownProps => (payload: IntegrationValues) => ({
    mutation: gql`
      mutation editCampaignBasics(
        $campaignId: String!
        $payload: CampaignInput!
      ) {
        editCampaign(id: $campaignId, campaign: $payload) {
          id
          externalSystem {
            id
          }
          isStarted
          syncReadiness
          readiness {
            id
            integration
          }
        }
      }
    `,
    variables: {
      campaignId: ownProps.campaignId,
      payload
    },
    refetchQueries: [
      {
        query: GET_SYNC_CONFIGS,
        variables: { campaignId: ownProps.campaignId }
      },
      {
        query: GET_SYNC_TARGETS,
        variables: { campaignId: ownProps.campaignId }
      }
    ]
  })
};

export default compose<InnerProps, RequiredComponentProps>(
  asSection({
    title: "Integration",
    readinessName: "integration",
    jobQueueNames: [],
    expandAfterCampaignStarts: false,
    expandableBySuperVolunteers: false
  }),
  loadData({
    queries,
    mutations
  })
)(CampaignIntegrationForm);
