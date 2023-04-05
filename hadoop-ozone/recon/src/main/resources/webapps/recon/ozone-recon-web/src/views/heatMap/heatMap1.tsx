import React, { Component } from 'react';
import { AgChartsReact } from 'ag-charts-react';

export default class heatMap1 extends Component {
  constructor(props: {} | Readonly<{}>) {
    super(props);
    const { data } = this.props;

    const colorRange1 = [
      '#ffff99',  //yellow start 80%
      '#ffff80',  //75%
      '#ffff66',  //70%
      '#ffff4d',  //yellow 65%
      '#ffd24d',  //dark Mustered yellow start 65%
      '#ffbf00',   //dark Mustard yellow end 50%
      '#b38600',  //Dark Mustard yellow 35%
      '#ffb366',  //orange start 70%
      '#ff9933',  //orange 60%
      '#ff8c1a',  //55%
      '#e67300',  //45%
      '#994d00',  //orange end 30%
      '#ff6633',  //Red start 60%
      '#ff4000',   // Red 50%
      '#cc3300',   //40%
      '#992600',   //30%
      '#802000',   //25%
      '#661a00',   //20%
      '#4d1300',   // 15%
      '#330000',    //dark Maroon
      '#330d00',   //10 % Last Red
    ];

    this.state = {
     // Tree Map Options Start
      options: {
        data,
        series: [
          {
          type: 'treemap',
          labelKey: 'label',// the name of the key to fetch the label value from
          sizeKey: 'size',  // the name of the key to fetch the value that will determine tile size     
          colorkey: 'color',
          fontSize: 35,
          title: { color: 'white', fontSize: 18, fontFamily:'Courier New' },
          subtitle: { color: 'white', fontSize: 15, fontFamily:'Courier New' },
          tooltip: {
            renderer: (params) => {
              return {
                content: `<span>
                Size:
                ${this.byteToSize(params.datum.size, 1) }
                 <br/>
                Access count:
                  ${params.datum.accessCount ? params.datum.accessCount : 'Not Defined'}
                  <br/>
                File Name:
                ${params.datum.label ? params.datum.label.split("/").slice(-1) : 'Key not Defined'}
                `,
              };
            },
          },
          formatter: ({ highlighted}) => {
            const stroke = highlighted ? 'red' : 'white';
            return { stroke };
          },
          labels: {
            color: 'white',
            fontWeight: 'bold',
            fontSize: 12
          },
          tileStroke: 'white',
          tileStrokeWidth: 1,
          colorDomain: [0.000, 0.050, 0.100, 0.150, 0.200, 0.250, 0.300, 0.350, 0.400, 0.450, 0.500, 0.550, 0.600, 0.650, 0.700, 0.750, 0.800, 0.850, 0.900, 0.950, 1.000],
          colorRange: [...colorRange1],
          groupFill: 'black',
          nodePadding: 1, //Disatnce between two nodes
          labelShadow: { enabled: false }, //labels shadow
          highlightStyle: {
            text: {
              color: 'red',
            },
          }
        }],
        title: { text: 'Volumes and Buckets'},
        }
    // Tree Map Options End
    }
  }
;

byteToSize = (bytes: number, decimals: number) => {
    if (bytes === 0) {
      return '0 Bytes';
    }
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return  isNaN(i) ? `Not Defined`:`${Number.parseFloat((bytes / (k ** i)).toFixed(dm))} ${sizes[i]}`;
  };

  render() {
    const { options } = this.state;
    return (
      <AgChartsReact options={options} />
    );
  }

}

