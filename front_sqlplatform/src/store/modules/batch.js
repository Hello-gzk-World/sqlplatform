import Vue from 'vue'
import router from '@/router'
import {resetRouter} from '@/router'
import {message, notification} from 'ant-design-vue'

import {
  runBatchAPI, commitAPI, getHintsAPI
} from "../../api/batch";

const batch = {
  state: {
    hint: ''
  },
  mutations: {
    set_hint: (state, data) => {
      state.hint = data;
      console.log(data)
      notification.warning({
        message: 'Didn\'t pass, please check!',
        description: data,
        duration: null,
      });
    },
  },
  actions: {
    runTest: async ({commit, dispatch}, data) => {
      const res = await runBatchAPI(data);
      if (res.msg === 'Passed') {
        notification.success({message: 'Passed!', duration: 2})
      } else if (res.msg === 'Didn\'t pass') {
        const res2 = await getHintsAPI({
          mainId: data.mainId,
          subId: data.subId,
          studentSql: data.batchText
        });
        commit('set_hint', res2.hints)
      }
    },
    commit: async ({commit, dispatch}, data) => {
      const res = await commitAPI(data);
      if (res.msg === 'Passed') {
        notification.success({
          message: 'Passed!',
          duration: 1,
          description: '已自动保存草稿'
        })
        commit('set_commit_state', {i: data.idx, state: true})
      } else if (res.msg === 'Didn\'t pass') {
        const res2 = await getHintsAPI({
          mainId: data.mainId,
          subId: data.subId,
          studentSql: data.batchText
        });
        message.success('已自动保存草稿')
        commit('set_hint', res2.hints)
      }
    },
  }
}

export default batch
