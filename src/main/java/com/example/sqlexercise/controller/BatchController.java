package com.example.sqlexercise.controller;

import com.example.sqlexercise.constant.Constants;
import com.example.sqlexercise.service.BatchService;
import com.example.sqlexercise.vo.BatchVO;
import com.example.sqlexercise.vo.ResponseVO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

//处理sql提交请求
//查询sql运行结果
@RestController
public class BatchController {

    private final BatchService batchService;

    @Autowired
    public BatchController(BatchService batchService) {
        this.batchService = batchService;
    }

    /**
     * 提交
     *
     * @return ResponseVO
     */
    @PostMapping("/api/batch/submit")
    public ResponseVO submitBatch(@RequestBody BatchVO batchVO) {
        String res = batchService.processBatch(batchVO, Constants.ProcessSqlMode.SUBMIT);
        return ResponseVO.success(res);
    }

    /**
     * 运行
     *
     * @return 返回单次运行结果给前端
     */
    @PostMapping("/api/batch/run")
    public ResponseVO runBatch(@RequestBody BatchVO batchVO) {
        String res = batchService.processBatch(batchVO, Constants.ProcessSqlMode.RUN);
        return ResponseVO.success(res);
    }

    @GetMapping("/api/batch/get")
    public ResponseVO getBatch(@RequestParam("batchId") String batchId) {
        return ResponseVO.success(batchService.getBatch(batchId));
    }

}
