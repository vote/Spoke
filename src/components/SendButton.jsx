import PropTypes from "prop-types";
import React, { Component } from "react";
import RaisedButton from "material-ui/RaisedButton";
import { StyleSheet, css } from "aphrodite";
import { dataTest } from "../lib/attributes";

// This is because the Toolbar from material-ui seems to only apply the correct margins if the
// immediate child is a Button or other type it recognizes. Can get rid of this if we remove material-ui
const styles = StyleSheet.create({
  container: {
    display: "inline-block",
    marginLeft: 24,
    marginBottom: 10
  }
});

class SendButton extends Component {
  state = {
    clickStepIndex: 0
  };


  render() {
    return (
      <div className={css(styles.container)}>
        <RaisedButton
          {...dataTest("send")}
          onTouchTap={this.props.onFinalTouchTap}
          disabled={this.props.disabled}
          label={this.props.label}
          primary
        />
      </div>
    );
  }
}

SendButton.propTypes = {
  label: PropTypes.string,
  onFinalTouchTap: PropTypes.func,
  disabled: PropTypes.bool
};

export default SendButton;
