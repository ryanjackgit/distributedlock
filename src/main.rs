
#![feature(map_first_last)]

use std::sync::Arc;

use std::time::Duration;

use zookeeper_async::{Acl, CreateMode, WatchedEvent, Watcher, ZooKeeper};
use zookeeper_async::KeeperState;
use zookeeper_async::ZooKeeperExt;

use tracing::{debug,info,warn,error};


use tokio::sync::oneshot;

use uuid::Uuid;



pub struct DistributedLock {
  zk:Arc<ZooKeeper>, 
  root_path:String,
  wait_pre_lock:Option<String>,
  current_lock:Option<String>,
}


impl DistributedLock {

    pub async fn new(address:String,root_path:String) -> anyhow::Result<Self> {
        debug!("connecting to ----- {}", address);



        let zk = ZooKeeper::connect(&address, Duration::from_secs(5),  move |event: WatchedEvent| {
            info!("{:?}", event);
            if event.keeper_state == KeeperState::Disconnected {
              
            }
        }).await?;
        

    zk.add_listener(|_zk_state| {} );

 

    let _auth = zk.add_auth("digest", vec![1, 2, 3, 4]).await;

/*
    let exists = zk.exists(&root_path, false).await?;
    if exists.is_none() {

    debug!("No this named node, begin to create---");
    if let Err(e) =zk.create(
            &root_path,
            vec![0],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .await {
            error!("created root error-> {:?}", e);
        }

       
    }
*/


    if let Err(e)=zk.ensure_path_with_leaf_mode(&root_path, CreateMode::Container).await {
        error!("don't create root is {:?}",e);
        return Err(e.into());
    }


     let d_lock =  DistributedLock {
            zk:Arc::new(zk),
            root_path,
            wait_pre_lock:None,
            current_lock:None,

        };

        Ok(d_lock)
    }

    pub async fn try_lock(&mut self) -> anyhow::Result<bool> {
     
       use std::collections::BTreeSet;

        let create_path=format!("{}/1",&self.root_path);
        let node_name = self.zk.create(
            &create_path,
            vec![0],
            Acl::open_unsafe().clone(),
            CreateMode::EphemeralSequential,
        )
        .await;

        if let Err(e)=node_name {
            error!("create children  error is {:?}",e);
            return Err(e.into());
        }

        
        self.current_lock=Some(node_name.unwrap());

        let mut children: Vec<String> = self.zk.get_children(&self.root_path, false).await?;
        children.sort();
        debug!("the list children is ---- {:?}",children);
        let mut tree_set=BTreeSet::new();

        for item in children {
            let path=format!("{}/{}",&self.root_path,item);
            tree_set.insert(path);
        }

        let first_node=tree_set.first();
      
    

        let mut flag=false;

        if self.current_lock.as_ref()==first_node {

           flag=true;
        }

        tree_set.retain(|item| Some(item)<self.current_lock.as_ref());

        if !tree_set.is_empty() {
            self.wait_pre_lock=tree_set.last().map(|item| item.clone());
        } 

        Ok(flag)

    }

    pub async fn lock(&mut self) -> anyhow::Result<()> {
  
        if self.try_lock().await? {
            debug!("get the trylock is true,got lock try lock");
            return Ok(());
        }
   
       
        // monitor the rev lock release,
        self.wait_lock(self.wait_pre_lock.clone().ok_or(anyhow::anyhow!("wait_per_lock is not set,error!"))?).await?;
        
        debug!("get the trylock is true,got lock after wait lock");
     
   
        Ok(())
    }

    pub async fn unlock(&mut self) -> anyhow::Result<()> {

        let unlock_path=self.current_lock.clone().ok_or(anyhow::anyhow!("self.current_lock is none "))?;

    
       if let Err(error) = self.zk.delete(&unlock_path, None).await {
        panic!("Couldn't remove lock {}, error is : {}", unlock_path, error);
    }
    
/*
    if let Err(e) = self.zk.close().await {
        warn!("the connection is not closed----------");
    };
*/

       self.current_lock = None;   
 
       debug!("unlock successful-------{}",unlock_path);
       Ok(())

    }

    async fn wait_lock(&mut self,name:String,) -> anyhow::Result<bool> {
     debug!("wait_lock() is called-------------waiting pre lock release: {:?}  ",name);
     let (tx, rx) = oneshot::channel();
  
    
        let stat = self.zk.exists_w(&name, move |e| {

            
            debug!("the event triggle is {:?}",e);
    
            if tx.send(()).is_err() {
                panic!("Error sending lock notification!");
            }
         })
        .await?;

        if stat.is_some() &&  rx.await.is_err() {
            return Err(anyhow::anyhow!("get oneshot message  error"));
        }


      debug!("get the trylock is true,got lock in wait_lock");
        
    Ok(true)

    }
}




struct LoggingWatcher();


impl Watcher for LoggingWatcher {

    fn handle(&self, e: WatchedEvent) {

      
        if e.keeper_state == KeeperState::Disconnected {
            debug!("Disconnected ------------------------------{:?}", e);
           
        }
       
    }
}


  fn main() -> anyhow::Result<()> {

    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var(
            "RUST_LOG",
            "distributelock=debug",
        )
    }

    tracing_subscriber::fmt::init();

    let root_path= Uuid::new_v4().to_string();
  
    let mut thread_handle=Vec::new();

    for i in 0..100 {
  
     let root_path=format!("/{}",root_path);
     let res=  std::thread::spawn(move || {
     
        let rt = tokio::runtime::Builder::new_current_thread()
    //    .worker_threads(1)
        .enable_all()
        .build();

      if let Ok(rt) =rt {
          let y=    lock_test(i,root_path.clone());
       if let Err(e)= rt.block_on(y) {
           error!("the erris {:?}",e);
       }
      }

     

       });

       thread_handle.push(res);
    }


  for item in thread_handle {
    item.join().unwrap();
  }

  Ok(())

   }


async fn lock_test(i:u32,root_path:String) ->anyhow::Result<()> {
    //zookeeper cluster ips
    let zookeeper_host="0.0.0.0:2181".to_string();

    let  lock=DistributedLock::new(zookeeper_host,root_path).await;

   if let Ok(mut lock) = lock {

   if let Err(e) =lock.lock().await {
       error!("the .lock().await  result e is {:?}",e);
   }

    info!("exclusive  do business  ------------------------- i:{} ",i);


    if let Err(e)=lock.unlock().await {
        error!("the unlock  is {:?}",e);
    }
   } else {
       error!("the DistributedLock object created error---------on one machine,too many connections,in real production,this isn't happen");
   }

    Ok(())

  }



