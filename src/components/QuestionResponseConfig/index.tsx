import React from "react";
import { compose } from "recompose";
import gql from "graphql-tag";
import { ApolloQueryResult } from "apollo-client";
import cloneDeep from "lodash/cloneDeep";

import { Card, CardHeader, CardText } from "material-ui/Card";
import { List } from "material-ui/List";
import Avatar from "material-ui/Avatar";
import RaisedButton from "material-ui/RaisedButton";
import DoneIcon from "material-ui/svg-icons/action/done";
import DeleteIcon from "material-ui/svg-icons/action/delete";
import AddBoxIcon from "material-ui/svg-icons/content/add-box";
import WarningIcon from "material-ui/svg-icons/alert/warning";
import BrokenIcon from "material-ui/svg-icons/image/broken-image";
import InfoIcon from "material-ui/svg-icons/action/info";
import NotificationPausedIcon from "material-ui/svg-icons/social/notifications-paused";
import {
  green200,
  orange200,
  grey400,
  red600,
  darkBlack
} from "material-ui/styles/colors";

import { RelayPaginatedResponse } from "../../api/pagination";
import {
  ExternalSyncQuestionResponseConfig,
  isActivistCode,
  isResponseOption,
  isResultCode
} from "../../api/external-sync-config";
import { GET_SYNC_CONFIGS } from "../SyncConfigurationModal/queries";
import { ExternalSurveyQuestion } from "../../api/external-survey-question";
import { ExternalActivistCode } from "../../api/external-activist-code";
import { ExternalResultCode } from "../../api/external-result-code";
import { loadData } from "../../containers/hoc/with-operations";
import { MutationMap } from "../../network/types";
import { ActivistCodeMapping } from "./components/ActivistCodeMapping";
import { ResponseOptionMapping } from "./components/ResponseOptionMapping";
import { ResultCodeMapping } from "./components/ResultCodeMapping";
import AddMapping from "../AddMapping";

interface HocProps {
  data: {};
  mutations: {
    deleteTarget(targetId: string): ApolloQueryResult<string>;
  };
}

interface OuterProps {
  campaignId: string;
  config: ExternalSyncQuestionResponseConfig;
  surveyQuestions: RelayPaginatedResponse<ExternalSurveyQuestion>;
  activistCodes: RelayPaginatedResponse<ExternalActivistCode>;
  resultCodes: RelayPaginatedResponse<ExternalResultCode>;
  style?: React.CSSProperties;
  createConfig(): void;
  deleteConfig(): void;
}

interface InnerProps extends OuterProps, HocProps {}

interface State {
  isAddMappingOpen: boolean;
}

class QuestionResponseConfig extends React.Component<InnerProps> {
  state: State = {
    isAddMappingOpen: false
  };

  makeHandleOnClickDeleteTarget = (targetId: string) => async () => {
    try {
      const response = await this.props.mutations.deleteTarget(targetId);
      if (response.errors) throw response.errors;
    } catch (err) {
      console.error(err);
    }
  };

  handleOnClickAddMapping = () => this.setState({ isAddMappingOpen: true });
  handleOnDismissAddMapping = () => this.setState({ isAddMappingOpen: false });

  render() {
    const {
      config,
      surveyQuestions,
      activistCodes,
      resultCodes,
      style
    } = this.props;
    const {
      questionResponseValue,
      includesNotActive,
      isMissing,
      isRequired,
      interactionStep: { questionText }
    } = config;

    const targets = config.targets;

    const avatar = includesNotActive ? (
      <Avatar
        icon={<BrokenIcon />}
        color={darkBlack}
        backgroundColor={orange200}
      />
    ) : isRequired ? (
      <Avatar
        icon={<WarningIcon />}
        color={darkBlack}
        backgroundColor={orange200}
      />
    ) : isMissing ? (
      <Avatar icon={<InfoIcon />} color={darkBlack} backgroundColor={grey400} />
    ) : targets && targets.length === 0 ? (
      <Avatar
        icon={<NotificationPausedIcon />}
        color={darkBlack}
        backgroundColor={green200}
      />
    ) : (
      <Avatar
        icon={<DoneIcon />}
        color={darkBlack}
        backgroundColor={green200}
      />
    );

    return (
      <Card
        expanded={false}
        onExpandChange={
          isMissing ? this.props.createConfig : this.props.deleteConfig
        }
        style={style}
      >
        <CardHeader
          title={questionResponseValue}
          subtitle={questionText}
          avatar={avatar}
          showExpandableButton={true}
          closeIcon={isMissing ? <AddBoxIcon /> : <DeleteIcon color={red600} />}
        />
        {!isMissing &&
          targets !== null && (
            <CardText>
              {targets.length > 0 && (
                <List>
                  {targets.map(target => {
                    if (isResponseOption(target)) {
                      return (
                        <ResponseOptionMapping
                          key={target.id}
                          responseOption={target.responseOption}
                          surveyQuestions={surveyQuestions}
                          onClickDelete={this.makeHandleOnClickDeleteTarget(
                            target.id
                          )}
                        />
                      );
                    } else if (isActivistCode(target)) {
                      return (
                        <ActivistCodeMapping
                          key={target.id}
                          activistCode={target.activistCode}
                          onClickDelete={this.makeHandleOnClickDeleteTarget(
                            target.id
                          )}
                        />
                      );
                    } else if (isResultCode(target)) {
                      return (
                        <ResultCodeMapping
                          key={target.id}
                          resultCode={target.resultCode}
                          onClickDelete={this.makeHandleOnClickDeleteTarget(
                            target.id
                          )}
                        />
                      );
                    }

                    return null;
                  })}
                </List>
              )}
              {targets.length === 0 && (
                <p>
                  This is an explicitly empty mapping that will be skipped
                  during sync.
                </p>
              )}
              <RaisedButton
                label="Add Mapping"
                primary={true}
                disabled={targets.length === 3}
                onClick={this.handleOnClickAddMapping}
              />
            </CardText>
          )}
        <AddMapping
          config={this.state.isAddMappingOpen ? config : undefined}
          surveyQuestions={surveyQuestions}
          activistCodes={activistCodes}
          resultCodes={resultCodes}
          existingTargets={targets || []}
          onRequestClose={this.handleOnDismissAddMapping}
        />
      </Card>
    );
  }
}

const mutations: MutationMap<InnerProps> = {
  deleteTarget: ownProps => (targetId: string) => ({
    mutation: gql`
      mutation deleteQuestionResponseSyncTarget($targetId: String!) {
        deleteQuestionResponseSyncTarget(targetId: $targetId)
      }
    `,
    variables: {
      targetId
    },
    update: store => {
      const variables = { campaignId: ownProps.campaignId };
      const data: any = cloneDeep(
        store.readQuery({
          query: GET_SYNC_CONFIGS,
          variables
        })
      );

      const configId = ownProps.config.id;
      const { edges } = data.campaign.externalSyncConfigurations;
      const index = edges.findIndex(({ node }) => node.id === configId);
      edges[index].node.targets = edges[index].node.targets.filter(
        target => target.id !== targetId
      );

      store.writeQuery({
        query: GET_SYNC_CONFIGS,
        variables,
        data
      });
    }
  })
};

export default compose<InnerProps, OuterProps>(loadData({ mutations }))(
  QuestionResponseConfig
);
