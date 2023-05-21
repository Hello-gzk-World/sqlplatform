<template>
  <div class="box" ref="box">
    <div class="left">
      <a-button
        style="float: right;margin: .8em 2.5em .8em .8em;"
        @click="back">返回
      </a-button>
      <v-md-preview :text="mainQuestion"></v-md-preview>

    </div>
    <div class="resize" title="收缩侧边栏">
    </div>
    <div class="mid">
      <div class="subQuestion">

        <a-tabs v-model="activeKey" style="text-align: left" @change="changePage(activeKey)">
          <a-tab-pane v-if="!loading"
                      :key="index" v-for="(item,index) in subQuestions">
            <template #tab>
              {{ '问题' + (index + 1) }} &nbsp
              <span class="passState" v-if="item.state">
                <a-tooltip placement="bottom" title="已通过">
                  <a-icon type="check-circle" style="color: #52c41a"></a-icon>
                </a-tooltip>
              </span>
              <span v-else class="passState">
                <a-tooltip placement="bottom" title="未通过">
                  <a-icon type="clock-circle" style="color: #faad14"></a-icon></a-tooltip>
              </span>
            </template>
            <div
              style="padding: 1em"
            >
              <a-tag v-if="item.difficulty == 1" color="green" class="tag">EASY</a-tag>
              <a-tag v-else-if="item.difficulty == 2" color="geekblue" class="tag">MEDIUM</a-tag>
              <a-tag v-else-if="item.difficulty == 3" color="volcano" class="tag">HARD</a-tag>

              {{ item.description }}
            </div>
            

            <div style="background-color: rgb(247,247,247);padding: 5px 1em 5px 2em;">
              <a-tag v-for="(tag,index) in item.tags"
                     color="blue"
                     :key="tag"
                     style="margin: 3px">
                {{ tag }}
              </a-tag>

              <div style="float: right;padding: 3px;align-items: center;display: flex">
                <span>深色模式&nbsp</span>
                  <a-switch v-model:checked="darkTheme"/>
              </div>

              <div style="float: right;padding: 3px;align-items: center;display: flex">
                <select v-model="selectedDriver">
                      <option v-for="driver in databaseDrivers" :value="driver">{{ driver }}</option>
                </select>
              </div>

            </div>
            <div class="commonEditor">
              <CommonEditor
                :value="codeSnippets[index]"
                language="sql"
                :theme="darkTheme ? 'darcula' : 'default'"
                @input="changeTextarea"
                style="height: 50vh"
              ></CommonEditor>
            </div>
            <div class="footbar">
              <span v-if="!buttonLoading" style="float: left;margin-left: 2em">
              <a-button @click="star(item,index)" v-if="!item.isStared"
                        style="color: #faad14;border-color: #faad14">
                <a-icon type="star"/>收藏</a-button>
              <a-button @click="unStar(item,index)" v-else style="color: #faad14;border-color: #faad14">
                <a-icon type="star" theme="filled"/>取消收藏</a-button>
              </span>
              <a-spin v-else style="float: left;margin-left: 2em"></a-spin>
              <a-button @click="save(item,index)"
                        style="float: left;margin-left: 1em"
              >
                <a-icon type="save"/>
                保存草稿
              </a-button>


              <a-button
                :disabled="runCd"
                @click="runCode(item,index)">
                <a-icon type="play-circle"/>
                {{runContent}}
              </a-button>

              <a-popover placement="bottom" content="将自动保存草稿">
                <!--                <template slot="content">-->
                <!--                  <p>将自动保存草稿</p>-->
                <!--                </template>-->
                <a-button style="background-color: mediumseagreen;color: white;border: 0;margin:0 1em 0 1em"
                          :disabled="submitCd"
                          @click="submitCode(item,index)">
                  <a-icon type="check-circle"/>
                  {{submitContent}}
                </a-button>
              </a-popover>


            </div>
            <div class="records">
              <p v-if="item.record.length === 0">暂无提交记录</p>
              <a-timeline>
                <a-timeline-item
                  v-for="(obj,idx) in item.record"
                  :key="idx"
                  :color="obj.passOrNot === '通过' ? 'green' : 'red'">
                  {{ obj.submitTime.replace('T', ' ') }}&nbsp&nbsp {{ obj.passOrNot }}
                </a-timeline-item>
              </a-timeline>
            </div>
          </a-tab-pane>
          <a-spin v-else tab=""></a-spin>
        </a-tabs>
      </div>
    </div>
  </div>
</template>

<script>
import {mapGetters, mapActions, mapMutations} from 'vuex'

import CommonEditor from '@/components/CommonEditor.vue'


import {ref} from 'vue';
import {message} from "ant-design-vue";

export default {
  name: "question",
  components: {CommonEditor},
  data() {
    return {
      page: 0,
      loading: true,
      codeSnippets: [],
      activeKey: ref(0),
      buttonLoading: false,
      darkTheme: false,
      runCd: false,
      submitCd: false,
      runContent: '运行',
      submitContent: '提交',
      restRunCd : 3,
      restSubmitCd : 3,
      runTimer: '',
      submitTimer: '',
      selectedDriver: 'MySQL',
      databaseDrivers: ['MySQL', 'OpenGauss', 'OceanBase'],
    }
  },
  async mounted() {
    await this.getUserInfoByToken();
    await this.getQuestion(this.$route.params.mainId)

    // 获取本题所有小题的缓存数据
    for (let i in this.subQuestions) {
      if (this.$route.params.subId === this.subQuestions[i].id) {
        this.activeKey = Number(i);
      }
      let data = {
        userId: this.userId,
        mainId: this.subQuestions[i].mainId,
        subId: this.subQuestions[i].id
      }
      await this.getDraft(data)
      data.idx = i;

      await this.getSubmitRecord(data)
      await this.getQuestionState(data)

    }
    this.codeSnippets = this.draft;

    this.dragControllerDiv();
    console.log(this.subQuestions)
    this.loading = false;
  },
  computed: {
    ...mapGetters([
      'mainQuestion', 'subQuestions', 'userId', 'subQuestions', 'draft', 'hint'
    ])
  },
  methods: {
    ...mapActions([
      'getUserInfoByToken',
      'getQuestion', 'runTest', 'commit',
      'saveDraft', 'getDraft',
      'getQuestionState',
      'starSubQuestion', 'unStarSubQuestion',
      'getSubmitRecord'
    ]),
    back() {
      let that = this;
      this.$confirm({
        okButtonProps: {props: {shape: 'round'}},
        cancelButtonProps: {props: {shape: 'round'}},
        title: '确认退出做题页面？',
        content: '若希望保存当前代码，请点击\"保存草稿\"，否则记录将丢失。',
        onOk() {
          that.$router.push({name: 'exerciseList'})
        },
        onCancel() {
        },
      });
    },
    changePage(key) {
      this.page = key;
    },
    dragControllerDiv() {
      var resize = document.getElementsByClassName('resize');
      var left = document.getElementsByClassName('left');
      var mid = document.getElementsByClassName('mid');
      var box = document.getElementsByClassName('box');
      for (let i = 0; i < resize.length; i++) {
        // 鼠标按下事件
        resize[i].onmousedown = function (e) {
          //颜色改变提醒
          resize[i].style.background = '#a8a5a5';
          var startX = e.clientX;
          resize[i].left = resize[i].offsetLeft;
          // 鼠标拖动事件
          document.onmousemove = function (e) {
            var endX = e.clientX;
            var moveLen = resize[i].left + (endX - startX); // （endx-startx）=移动的距离。resize[i].left+移动的距离=左边区域最后的宽度
            var maxT = box[i].clientWidth - resize[i].offsetWidth; // 容器宽度 - 左边区域的宽度 = 右边区域的宽度

            if (moveLen < 32) moveLen = 32; // 左边区域的最小宽度为32px
            if (moveLen > maxT - 150) moveLen = maxT - 150; //右边区域最小宽度为150px

            resize[i].style.left = moveLen; // 设置左侧区域的宽度

            for (let j = 0; j < left.length; j++) {
              left[j].style.width = moveLen + 'px';
              mid[j].style.width = (box[i].clientWidth - moveLen - 10) + 'px';
            }
          };
          // 鼠标松开事件
          document.onmouseup = function (evt) {
            //颜色恢复
            resize[i].style.background = '#d6d6d6';
            document.onmousemove = null;
            document.onmouseup = null;
            resize[i].releaseCapture && resize[i].releaseCapture(); //当你不在需要继续获得鼠标消息就要应该调用ReleaseCapture()释放掉
          };
          resize[i].setCapture && resize[i].setCapture(); //该函数在属于当前线程的指定窗口里设置鼠标捕获
          return false;
        };
      }
    },
    changeTextarea(val) {
      this.codeSnippets[this.page] = val
    },

    runCode(item, index) {
      if (this.runCd) return;
      this.runTest({
        batchText: this.codeSnippets[index],
        userId: this.userId,
        mainId: item.mainId,
        subId: item.id,
        //todo
        driver: 'mysql'
      });
      this.runCd = true;
      this.runContent = this.restRunCd + 's后可重新运行';
      this.runTimer = setInterval(() => {
        this.restRunCd--
        this.runContent = this.restRunCd + 's后可重新运行';
        if (this.restRunCd < 0) {
          clearInterval(this.runTimer)
          this.runContent = '运行'
          this.restRunCd = 3
          this.runCd = false
        }
      }, 1000)
    },
    async submitCode(item, index) {
      if (this.submitCd) return;
      await this.commit({
        batchText: this.codeSnippets[index],
        userId: this.userId,
        mainId: item.mainId,
        subId: item.id,
        driver: 'mysql',
        idx: index
      })
      this.submitCd = true;
      this.submitContent = this.restSubmitCd + 's后可重新提交';
      this.submitTimer = setInterval(() => {
        this.restSubmitCd--
        this.submitContent = this.restSubmitCd + 's后可重新提交';
        if (this.restSubmitCd < 0) {
          clearInterval(this.submitTimer)
          this.submitContent = '提交'
          this.restSubmitCd = 3
          this.submitCd = false
        }
      }, 1000)
      this.loading = true
      await this.getSubmitRecord({
        userId: this.userId,
        mainId: item.mainId,
        subId: item.id,
        idx: index
      })
      this.loading = false;
    },
    save(item, index) {
      this.saveDraft({
        userId: this.userId,
        mainId: item.mainId,
        subId: item.id,
        draft: this.codeSnippets[index]
      })
    },
    async star(item, index) {
      this.buttonLoading = true;
      await this.starSubQuestion({
        userId: this.userId,
        mainId: item.mainId,
        subId: item.id,
        idx: index
      })
      this.buttonLoading = false;
    },
    async unStar(item, index) {
      this.buttonLoading = true;
      await this.unStarSubQuestion({
        userId: this.userId,
        mainId: item.mainId,
        subId: item.id,
        idx: index
      })
      this.buttonLoading = false;
    },
  }
}
</script>

<style scoped>
/deep/ .ant-btn {
  /*color: rgb(81,104,148);*/
  /*border-color: rgb(81,104,148);*/
  background-color: transparent;
}

.tag {
  margin-bottom: 3px;
}

.box {
  width: 100%;
  height: 100%;
  overflow: hidden;
}

/*左侧div样式*/
.left {
  width: calc(40% - 8px); /*左侧初始化宽度*/
  height: 100vh;
  /*background: #c74c4c;*/
  float: left;
  text-align: left;
  overflow: auto;
  box-shadow: 1px 1px 5px 2px rgba(0, 0, 0, 0.11);

}

/*拖拽区div样式*/
.resize {
  cursor: col-resize;
  float: left;
  position: relative;
  /*top: 45vh;*/
  background-color: #d6d6d6;
  /*border-radius: 5px;*/
  /*margin-top: -10px;*/
  /*top: 2vh;*/
  width: 8px;
  height: 100vh;
  background-size: cover;
  background-position: center;
  /*z-index: 99999;*/
  font-size: 32px;
  /*color: white;*/
}

/*!*拖拽区鼠标悬停样式*!*/
/*.resize:hover {*/
/*  background-color: #868686;*/
/*}*/
/*右侧div'样式*/
.mid {
  float: left;
  width: 60%; /*右侧初始化宽度*/
  height: 100vh;
  /*background: #226fa1;*/
  box-shadow: 1px 1px 5px 2px rgba(0, 0, 0, 0.11);
  overflow: auto;
}

.subQuestion {
  height: 20vh;
  margin: 10px;
}

.commonEditor {
  text-align: left;
}

.footbar {
  background-color: rgb(247, 247, 247);
  /*height: 2em;*/
  float: bottom;
  padding: 6px;
  text-align: right;
}

.records {
  height: 16vh;
  overflow: auto;
  padding: 1em;
  background-color: rgb(247, 247, 247);
  margin-top: 1em;;
}

.passState {
  border-radius: 5px;
}

</style>

