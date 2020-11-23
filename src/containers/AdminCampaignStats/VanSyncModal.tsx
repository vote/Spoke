import React from "react";
import { History } from "history";
import { withRouter } from "react-router";
import { compose } from "recompose";
import gql from "graphql-tag";
import { ApolloQueryResult } from "apollo-client";

import Dialog from "material-ui/Dialog";
import FlatButton from "material-ui/FlatButton";
import RaisedButton from "material-ui/RaisedButton";
import { red500, green300 } from "material-ui/styles/colors";

import { ExternalSyncReadinessState } from "../../api/campaign";
import { loadData } from "../hoc/with-operations";
import { QueryMap, MutationMap } from "../../network/types";
import { Campaign, JobRequest } from "../../api/campaign";
import SyncConfigurationModal from "../../components/SyncConfigurationModal";

interface JobRequestResult {
  message: string;
  error_counts?: {
    [key: string]: number;
  };
}

const messageFromJobRquest = (job: JobRequest) => {
  try {
    const result: JobRequestResult = JSON.parse(job.resultMessage);
    return result.message;
  } catch {
    return `Error: could not parse result for job_request ${job.id}`;
  }
};

const errorListFromJobRequest = (job: JobRequest) => {
  let result: JobRequestResult;
  try {
    result = JSON.parse(job.resultMessage);
  } catch {
    return [`Error: could not parse result for job_request ${job.id}`];
  }

  if (result.error_counts) {
    return Object.entries(result.error_counts).map(
      ([error, error_count]) => `${error_count} - ${error}`
    );
  }

  return [];
};

interface HocProps {
  data: {
    campaign: Pick<Campaign, "id" | "syncReadiness">;
  };
  syncJobs: {
    campaign: Pick<Campaign, "id" | "pendingJobs">;
  };
  history: History;
  mutations: {
    syncCampaign(): ApolloQueryResult<boolean>;
    cancelSync(jobId: string): ApolloQueryResult<{ id: string }>;
  };
}

interface OuterProps {
  open: boolean;
  organizationId: string;
  campaignId: string;
  onRequestClose(): void;
  onComplete(): void;
}

interface InnerProps extends OuterProps, HocProps {}

interface State {
  isMappingOpen: boolean;
  isWorking: boolean;
}

class VanSyncModal extends React.Component<InnerProps, State> {
  state: State = {
    isMappingOpen: false,
    isWorking: false
  };

  handleOnClickSetIntegration = async () => {
    const { history, organizationId, campaignId } = this.props;
    this.props.onComplete();
    history.push(`/admin/${organizationId}/campaigns/${campaignId}/edit`);
  };

  handleOnClickConfigureMapping = async () =>
    this.setState({ isMappingOpen: true });
  handleOnDismissConfigureMapping = async () =>
    this.setState({ isMappingOpen: false });

  handleOnConfirmSync = async () => {
    this.setState({ isWorking: true });
    try {
      const response = await this.props.mutations.syncCampaign();
      if (response.errors) throw response.errors;
      this.props.onComplete();
    } catch (err) {
      console.error("error syncing campaign", err);
    } finally {
      this.setState({ isWorking: false });
    }
  };

  handleOnCancelSync = (jobId: string) => async () => {
    this.setState({ isWorking: true });
    try {
      const response = await this.props.mutations.cancelSync(jobId);
      if (response.errors) throw response.errors;
    } catch (err) {
      console.error("error cancelling campaign sync", err);
    } finally {
      this.setState({ isWorking: false });
    }
  };

  render() {
    const { isMappingOpen, isWorking } = this.state;
    const { open, organizationId, campaignId, data } = this.props;
    const {
      campaign: { syncReadiness }
    } = data;

    const latestSyncAttempt = this.props.syncJobs.campaign.pendingJobs[0];
    const isSyncing: boolean =
      latestSyncAttempt && latestSyncAttempt.status !== 100;
    const syncErrors = latestSyncAttempt
      ? errorListFromJobRequest(latestSyncAttempt)
      : [];

    const isSyncDisabled =
      isWorking ||
      syncReadiness !== ExternalSyncReadinessState.READY ||
      isSyncing;

    const actions = [
      <FlatButton label="Cancel" onClick={this.props.onRequestClose} />,
      <FlatButton
        label="Sync"
        primary={true}
        disabled={isSyncDisabled}
        onClick={this.handleOnConfirmSync}
      />
    ];

    return (
      <Dialog
        open={open}
        title="Sync to VAN"
        actions={actions}
        onRequestClose={this.props.onRequestClose}
      >
        <p>
          This will sync question responses and tags to VAN. For more
          information see{" "}
          <a
            href="https://docs.spokerewired.com/article/93-van-list-loading"
            target="_blank"
          >
            VAN Integration
          </a>
          .
        </p>
        {isSyncing ? (
          <div>
            <p>Syncing campaign: {latestSyncAttempt.status}%</p>
            {syncErrors.length > 0 && [
              <p>Encountered the following errors</p>,
              <ul>{syncErrors.map(error => <li key={error}>{error}</li>)}</ul>
            ]}
            <RaisedButton
              label="Cancel Sync"
              primary={true}
              onClick={this.handleOnCancelSync(latestSyncAttempt.id)}
            />
            <br />
            <br />
          </div>
        ) : (
          <p>
            Status:{" "}
            {syncReadiness === ExternalSyncReadinessState.READY && (
              <span style={{ color: green300 }}>
                Your campaign is ready to sync!
              </span>
            )}
            {syncReadiness ===
              ExternalSyncReadinessState.MISSING_REQUIRED_MAPPING && (
              <span style={{ color: red500 }}>
                Your campaign is missing a required sync mapping!
              </span>
            )}
            {syncReadiness ===
              ExternalSyncReadinessState.INCLUDES_NOT_ACTIVE_TARGETS && (
              <span style={{ color: red500 }}>
                Your campaign includes mappings to inactive or archived VAN
                options!
              </span>
            )}
            {syncReadiness === ExternalSyncReadinessState.MISSING_SYSTEM && (
              <span style={{ color: red500 }}>
                No integration has been set for this campaign!
              </span>
            )}
          </p>
        )}
        {syncReadiness === ExternalSyncReadinessState.MISSING_SYSTEM && [
          <p key="1">Edit the Integration section of the campaign.</p>,
          <RaisedButton
            key="2"
            label="Edit Campaign"
            primary={true}
            disabled={isSyncing}
            onClick={this.handleOnClickSetIntegration}
          />
        ]}
        {syncReadiness !== ExternalSyncReadinessState.MISSING_SYSTEM && (
          <RaisedButton
            label="Configure Mapping"
            primary={true}
            disabled={isSyncing}
            onClick={this.handleOnClickConfigureMapping}
          />
        )}
        {isMappingOpen && (
          <SyncConfigurationModal
            organizationId={organizationId}
            campaignId={campaignId}
            onRequestClose={this.handleOnDismissConfigureMapping}
          />
        )}
        {latestSyncAttempt &&
          latestSyncAttempt.status === 100 && (
            <p>
              Last sync:<br />
              {new Date(latestSyncAttempt.updatedAt).toLocaleString()} -{" "}
              {messageFromJobRquest(latestSyncAttempt)}
            </p>
          )}
      </Dialog>
    );
  }
}

const GET_CAMPAIGN_SYNC_JOBS = gql`
  query getCampaignSyncJobs($campaignId: String!, $jobTypes: [String]) {
    campaign(id: $campaignId) {
      id
      pendingJobs(jobTypes: $jobTypes) {
        id
        jobType
        status
        resultMessage
        createdAt
        updatedAt
      }
    }
  }
`;

const queries: QueryMap<InnerProps> = {
  data: {
    query: gql`
      query getCampaignSyncReadiness($campaignId: String!) {
        campaign(id: $campaignId) {
          id
          syncReadiness
        }
      }
    `,
    options: ownProps => ({
      variables: {
        campaignId: ownProps.campaignId
      }
    })
  },
  syncJobs: {
    query: GET_CAMPAIGN_SYNC_JOBS,
    options: ownProps => ({
      variables: {
        campaignId: ownProps.campaignId,
        jobTypes: ["sync_van_campaign"]
      },
      pollInterval: 10 * 1000
    })
  }
};

const mutations: MutationMap<InnerProps> = {
  syncCampaign: ownProps => () => ({
    mutation: gql`
      mutation syncCampaignToSystem($input: SyncCampaignToSystemInput!) {
        syncCampaignToSystem(input: $input)
      }
    `,
    variables: {
      input: {
        campaignId: ownProps.campaignId
      }
    }
  }),
  cancelSync: ownProps => (jobId: string) => ({
    mutation: gql`
      mutation cancelCampaignVanSync($campaignId: String!, $jobId: String!) {
        deleteJob(campaignId: $campaignId, id: $jobId) {
          id
        }
      }
    `,
    variables: {
      campaignId: ownProps.campaignId,
      jobId
    },
    refetchQueries: [
      {
        query: GET_CAMPAIGN_SYNC_JOBS,
        variables: {
          campaignId: ownProps.campaignId,
          jobTypes: ["sync_van_campaign"]
        }
      }
    ]
  })
};

export default compose<InnerProps, OuterProps>(
  withRouter,
  loadData({ queries, mutations })
)(VanSyncModal);
