/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
import { Component } from 'react'
import { connect } from 'react-redux'

import { canUseDOM } from 'services/utils'
import { inDesktop } from 'shared/modules/app/appDuck'
import {
  getAuraNtId,
  getDesktopTrackingId,
  updateUdcData
} from 'shared/modules/udc/udcDuck'

export interface MetricsProperties {
  [key: string]: string | number | Date | boolean
}

export interface MetricsData {
  category: string
  label: string
  data: MetricsProperties
}

export class Segment extends Component<any> {
  componentDidMount() {
    const {
      segmentKey,
      setTrackCallback,
      inDesktop,
      updateData,
      auraNtId,
      desktopTrackingId,
      children, // eslint-disable-line
      ...otherProps
    } = this.props
    if (!segmentKey || !canUseDOM()) {
      return
    }

    updateData({ ...otherProps, segmentKey: segmentKey })
  }

  componentDidUpdate() {
    const {
      segmentKey,
      updateData,
      children, // eslint-disable-line
      ...otherProps
    } = this.props
    if (!canUseDOM()) return
    updateData({ ...otherProps, segmentKey: segmentKey })
  }

  shouldComponentUpdate() {
    return false
  }

  componentWillUnmount() {
    const { setTrackCallback } = this.props
    setTrackCallback(null)
    if (canUseDOM()) {
      delete (window as any).analytics
    }
  }

  render() {
    return null
  }
}

const mapStateToProps = (state: any) => ({
  inDesktop: inDesktop(state),
  auraNtId: getAuraNtId(state),
  desktopTrackingId: getDesktopTrackingId(state)
})

const mapDispatchToProps = (dispatch: any) => {
  return {
    updateData: (data: any) => dispatch(updateUdcData(data))
  }
}
export default connect<any, any, any, any>(
  mapStateToProps,
  mapDispatchToProps
)(Segment)
