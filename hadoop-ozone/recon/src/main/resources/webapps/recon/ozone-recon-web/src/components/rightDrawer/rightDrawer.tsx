import React from 'react';
import { Drawer, Button } from 'antd';

export class DetailPanel extends React.Component {
  state = { visible: false};

  componentWillReceiveProps(props) {
    this.setState({ visible: props.visible })
  }

  onClose = () => {
    this.setState({
      visible: false,
    });
  };

  render() {
    return (
      <div className="site-drawer-render-in-current-wrapper">
        <Drawer
          title= {"Metadata Summary for " + this.props.path}
          placement="right"
          width="40%"
          closable={false}
          onClose={this.onClose}
          visible={this.state.visible}
          getContainer={false}
          style={{ position: 'absolute' }}
        >
        </Drawer>
      </div>
    );
  }
}