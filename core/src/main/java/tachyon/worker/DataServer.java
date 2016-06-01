package tachyon.worker;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.nio.ByteBuffer;

import org.accelio.jxio.*;
import org.accelio.jxio.exceptions.*;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.JXIO.xio_DataServerMessage;
import tachyon.Users;
import tachyon.conf.UserConf;

/**
 * Created by yuyang on 2014/10/8.
 */

/**
 * The Server to serve data file read request from remote machine
 */

public class DataServer implements Runnable {
    private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);


    private final BlocksLocker mBlocksLocker;


    private volatile boolean mShutdown = false;
    private volatile boolean mShutdowned = false;

    /**
     * 以下是JXIO使用到的成员
     */
    private MsgPool mp;
    private ServerPortal DataServerPortal;
    private EventQueueHandler mEQH;
    private ServerSession DataServerSession;

    /**
     * 构造函数
     * @param address
     * @param workerStorage
     */
    public DataServer(InetSocketAddress address,WorkerStorage workerStorage){
        LOG.info("Starting DataServer @" + address);
        mBlocksLocker = new BlocksLocker(workerStorage, Users.sDATASERVER_USER_ID);

        URI uri = null;
        try{
            uri = new URI("rdma://" + address.getAddress().getHostAddress() + ":" + address.getPort() + "/");
        }catch (URISyntaxException e) {
            e.printStackTrace();

        }

        this.mEQH = new EventQueueHandler(null);
        this.mp = new MsgPool(1+64,xio_DataServerMessage.HEADER_LENGTH, xio_DataServerMessage.HEADER_LENGTH+UserConf.get().REMOTE_READ_BUFFER_SIZE_BYTE);
        this.mEQH.bindMsgPool(mp);
        this.DataServerPortal = new ServerPortal(mEQH,uri,new DataServerPortalCallbacks());

    }

//以下是DataServer的接口函数定义
    public boolean isClosed(){
        return mShutdowned;
    }

    /**
     * 关闭DataServer的函数
     * @return
     */
    public void close(){
        DataServerPortal.close();
        mShutdown = true;
    }

    public void releaseResource(){
        mEQH.releaseMsgPool(mp);
        mp.deleteMsgPool();
        mEQH.close();
    }

    public void run(){

        mEQH.run();
        mShutdowned =true;
    }

    private boolean ProcessMsg(Msg msg){
        boolean ret = false;

        do {

            ByteBuffer requestMsg = msg.getIn();
            ByteBuffer MsgHeader = xio_DataServerMessage.GetHeader(requestMsg);
            if(null == MsgHeader){
                LOG.error("The header of request message from client is invalid !");
                ret = false;
                break;
            }
            long BlockId = xio_DataServerMessage.getBlockIdFromHeader(MsgHeader);
            if(BlockId < 0){
                LOG.error("Invalid block id " + BlockId);
                ret = false;
                break;
            }
            long Offset = xio_DataServerMessage.getOffsetFromHeader(MsgHeader);
            if(Offset < 0){
                LOG.error("Invalid offset " + Offset);
                ret = false;
                break;
            }
            long length = xio_DataServerMessage.getLengthFromHeader(MsgHeader);
            if(length < 0){
                LOG.error("Invalid length " + length);
                ret = false;
                break;
            }

            int lockId = mBlocksLocker.lock(BlockId);
            DataServerMessage dataServerMessageResponseMessage = DataServerMessage.createBlockResponseMessage(true,BlockId,Offset,length);
            ByteBuffer ResponseMessagePacket = xio_DataServerMessage.CreateBlockResponseMessagePacket(dataServerMessageResponseMessage.getBlockId(),
                    dataServerMessageResponseMessage.getOffset(),
                    dataServerMessageResponseMessage.getLength(),
                    dataServerMessageResponseMessage.getmData(),
                    lockId);

            //开始发送回应数据
            msg.getOut().put(ResponseMessagePacket);
            //Msg  responseMsg= mp.getMsg();
            //responseMsg.getOut().put(ResponseMessagePacket);

            try {
                DataServerSession.sendResponse(msg);
                //responseMsg.returnToParentPool();
            }catch(JxioGeneralException e){
                LOG.error("Error sending :" + e.toString());
                mp.releaseMsg(msg);
                ret = false;
                break;
            }catch(JxioSessionClosedException e){
                LOG.error("Error sending : session closed " + e.toString());
                mp.releaseMsg(msg);
                ret = false;
                break;
            }

            dataServerMessageResponseMessage.close();
            mBlocksLocker.unlock(Math.abs(BlockId), lockId);
            ret = true;
        }while(false);

        return ret;
    }

    public class DataServerPortalCallbacks implements ServerPortal.Callbacks{
        public void onSessionEvent(EventName event,EventReason reason){
            if(event == EventName.PORTAL_CLOSED) {
                LOG.info("This session is closed,begin to release the resource!");
                mEQH.stop();
                releaseResource();
            }
        }
        public void onSessionNew(ServerSession.SessionKey seskey,String srcIp,WorkerCache.Worker hint){
            DataServerSession = new ServerSession(seskey,new DataServerSessionCallbacks());
            DataServerPortal.accept(DataServerSession);
        }
    }

    public class DataServerSessionCallbacks implements ServerSession.Callbacks{

        public boolean onMsgError(Msg msg,EventReason reason){
            LOG.error("On Message Error,Reason is :" + reason.toString());
            return DataServerSession.close();
        }

        public void onRequest(Msg msg){
            if(ProcessMsg(msg))
            {
                LOG.info("[Success]Begin to close the session!");
            }
            else{
                LOG.info("[FAILED] Begin to close the session");
            }
        }

        public void onSessionEvent(EventName event,EventReason reason){
            if(event == EventName.SESSION_CLOSED){
                LOG.info("The session is closed, because of " + reason.toString());
            }
            if(event == EventName.SESSION_ERROR){
                LOG.error("The session occur a error,because of "+ reason.toString());
                DataServerSession.close();
            }
        }
    }

}
