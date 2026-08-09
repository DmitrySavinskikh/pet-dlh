"""
GameGuard Lakehouse - Synthetic Telemetry Generator

Generates synthetic PC game telemetry events including:
- Normal player sessions
- Predefined cheater scenarios (aimbot, speedhack, wallhack patterns)

Events are sent to Kafka topic: raw.telemetry
"""

import json
import uuid
import time
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from kafka import KafkaProducer
from kafka.errors import KafkaError


@dataclass
class GameEvent:
    """Base event schema for all telemetry events."""
    event_id: str
    event_time: str  # ISO format timestamp
    player_id: str
    session_id: str
    match_id: str
    event_type: str
    # Game metrics
    position_x: float = 0.0
    position_y: float = 0.0
    position_z: float = 0.0
    rotation_yaw: float = 0.0
    rotation_pitch: float = 0.0
    health: int = 100
    ammo: int = 30
    weapon_id: str = "rifle_01"
    # Additional context
    team_id: int = 1
    is_alive: bool = True
    kills: int = 0
    deaths: int = 0
    assists: int = 0
    damage_dealt: int = 0
    damage_taken: int = 0
    movement_speed: float = 5.0  # m/s, normal walking speed
    reaction_time_ms: int = 250  # normal human reaction time
    accuracy: float = 0.3  # 30% accuracy is reasonable
    headshot_ratio: float = 0.15  # 15% headshots is good
    view_angle_change: float = 0.0  # degrees per frame
    distance_traveled: float = 0.0
    # Metadata
    client_version: str = "1.0.0"
    region: str = "eu-west"
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))


class TelemetryGenerator:
    """Generates synthetic game telemetry with normal and cheater patterns."""
    
    # Cheat detection thresholds (will be used by downstream processing)
    CHEAT_THRESHOLDS = {
        'max_accuracy': 0.85,  # >85% accuracy is suspicious
        'min_reaction_time_ms': 100,  # <100ms is superhuman
        'max_movement_speed': 15.0,  # >15 m/s is speedhack
        'max_headshot_ratio': 0.6,  # >60% headshots is suspicious
        'max_view_angle_change': 90.0,  # instant 90° turn is aimbot
    }
    
    def __init__(self, kafka_bootstrap_servers: List[str], topic: str = "raw.telemetry"):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: v.encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
        )
        self.active_players: Dict[str, Dict[str, Any]] = {}
        self.match_counter = 0
        
    def _generate_event_id(self) -> str:
        return str(uuid.uuid4())
    
    def _generate_player_id(self) -> str:
        return f"player_{random.randint(1000, 9999)}"
    
    def _generate_session_id(self) -> str:
        return str(uuid.uuid4())[:8]
    
    def _generate_match_id(self) -> str:
        self.match_counter += 1
        return f"match_{self.match_counter}"
    
    def _get_current_timestamp(self) -> str:
        return datetime.utcnow().isoformat() + "Z"
    
    def create_normal_player_session(self) -> Dict[str, Any]:
        """Create a new normal player session."""
        player_id = self._generate_player_id()
        session_id = self._generate_session_id()
        match_id = self._generate_match_id()
        
        self.active_players[player_id] = {
            'session_id': session_id,
            'match_id': match_id,
            'start_time': datetime.utcnow(),
            'is_cheater': False,
            'position': [0.0, 0.0, 0.0],
            'kills': 0,
            'deaths': 0,
            'total_shots': 0,
            'total_hits': 0,
            'headshots': 0,
        }
        
        return self.active_players[player_id]
    
    def create_cheater_session(self, cheat_type: str) -> Dict[str, Any]:
        """Create a new cheater session with specific cheat type."""
        player_id = self._generate_player_id()
        session_id = self._generate_session_id()
        match_id = self._generate_match_id()
        
        self.active_players[player_id] = {
            'session_id': session_id,
            'match_id': match_id,
            'start_time': datetime.utcnow(),
            'is_cheater': True,
            'cheat_type': cheat_type,
            'position': [0.0, 0.0, 0.0],
            'kills': 0,
            'deaths': 0,
            'total_shots': 0,
            'total_hits': 0,
            'headshots': 0,
        }
        
        return self.active_players[player_id]
    
    def generate_normal_event(self, player_data: Dict[str, Any]) -> GameEvent:
        """Generate a normal player event with realistic human behavior."""
        player_id = player_data['session_id']
        
        # Simulate natural movement
        dx = random.uniform(-2.0, 2.0)
        dy = random.uniform(-2.0, 2.0)
        dz = random.uniform(-1.0, 1.0)
        player_data['position'][0] += dx
        player_data['position'][1] += dy
        player_data['position'][2] += dz
        
        # Natural variation in accuracy (20-40%)
        accuracy = random.uniform(0.20, 0.40)
        
        # Human reaction time (150-350ms)
        reaction_time = random.randint(150, 350)
        
        # Normal movement speed (3-7 m/s)
        movement_speed = random.uniform(3.0, 7.0)
        
        # Natural headshot ratio (10-25%)
        headshot_ratio = random.uniform(0.10, 0.25)
        
        # Small view angle changes
        view_angle_change = random.uniform(0.0, 30.0)
        
        # Occasional kill/death
        is_kill = random.random() < 0.1
        is_death = random.random() < 0.05
        
        if is_kill:
            player_data['kills'] += 1
            if random.random() < headshot_ratio:
                player_data['headshots'] += 1
        
        if is_death:
            player_data['deaths'] += 1
        
        player_data['total_shots'] += 1
        if random.random() < accuracy:
            player_data['total_hits'] += 1
        
        return GameEvent(
            event_id=self._generate_event_id(),
            event_time=self._get_current_timestamp(),
            player_id=player_id,
            session_id=player_data['session_id'],
            match_id=player_data['match_id'],
            event_type="player_action",
            position_x=player_data['position'][0],
            position_y=player_data['position'][1],
            position_z=player_data['position'][2],
            rotation_yaw=random.uniform(0, 360),
            rotation_pitch=random.uniform(-90, 90),
            health=random.randint(50, 100) if not is_death else 0,
            ammo=random.randint(0, 30),
            weapon_id=random.choice(["rifle_01", "sniper_01", "pistol_01"]),
            team_id=random.randint(1, 2),
            is_alive=not is_death,
            kills=player_data['kills'],
            deaths=player_data['deaths'],
            assists=random.randint(0, 3),
            damage_dealt=random.randint(0, 100) if is_kill else 0,
            damage_taken=random.randint(0, 80) if is_death else 0,
            movement_speed=movement_speed,
            reaction_time_ms=reaction_time,
            accuracy=accuracy,
            headshot_ratio=headshot_ratio,
            view_angle_change=view_angle_change,
            distance_traveled=random.uniform(0, 10),
        )
    
    def generate_cheater_event(self, player_data: Dict[str, Any], cheat_type: str) -> GameEvent:
        """Generate a cheater event with anomalous patterns."""
        player_id = player_data['session_id']
        
        if cheat_type == "aimbot":
            # Aimbot: instant snaps, perfect accuracy, high headshot ratio
            accuracy = random.uniform(0.85, 1.0)
            reaction_time = random.randint(50, 100)  # Superhuman
            headshot_ratio = random.uniform(0.6, 0.9)
            view_angle_change = random.uniform(90, 180)  # Instant snap
            movement_speed = random.uniform(3.0, 6.0)
            
        elif cheat_type == "speedhack":
            # Speedhack: impossible movement speed
            accuracy = random.uniform(0.3, 0.5)
            reaction_time = random.randint(150, 250)
            headshot_ratio = random.uniform(0.15, 0.3)
            view_angle_change = random.uniform(0, 30)
            movement_speed = random.uniform(20.0, 50.0)  # Impossible speed
            
        elif cheat_type == "wallhack":
            # Wallhack: pre-aiming through walls, tracking enemies
            accuracy = random.uniform(0.7, 0.85)
            reaction_time = random.randint(80, 120)  # Very fast
            headshot_ratio = random.uniform(0.4, 0.6)
            view_angle_change = random.uniform(45, 90)
            movement_speed = random.uniform(5.0, 8.0)
            
        else:
            # Default cheater pattern
            accuracy = random.uniform(0.6, 0.8)
            reaction_time = random.randint(100, 150)
            headshot_ratio = random.uniform(0.4, 0.6)
            view_angle_change = random.uniform(60, 120)
            movement_speed = random.uniform(8.0, 15.0)
        
        # Update position based on speed
        speed_factor = movement_speed / 5.0  # Normalize to normal speed
        dx = random.uniform(-2.0, 2.0) * speed_factor
        dy = random.uniform(-2.0, 2.0) * speed_factor
        dz = random.uniform(-1.0, 1.0) * speed_factor
        player_data['position'][0] += dx
        player_data['position'][1] += dy
        player_data['position'][2] += dz
        
        # Cheaters get more kills
        is_kill = random.random() < 0.4
        is_death = random.random() < 0.02  # Cheaters rarely die
        
        if is_kill:
            player_data['kills'] += 1
            if random.random() < headshot_ratio:
                player_data['headshots'] += 1
        
        if is_death:
            player_data['deaths'] += 1
        
        player_data['total_shots'] += 1
        if random.random() < accuracy:
            player_data['total_hits'] += 1
        
        return GameEvent(
            event_id=self._generate_event_id(),
            event_time=self._get_current_timestamp(),
            player_id=player_id,
            session_id=player_data['session_id'],
            match_id=player_data['match_id'],
            event_type="player_action",
            position_x=player_data['position'][0],
            position_y=player_data['position'][1],
            position_z=player_data['position'][2],
            rotation_yaw=random.uniform(0, 360),
            rotation_pitch=random.uniform(-90, 90),
            health=random.randint(80, 100) if not is_death else 0,
            ammo=random.randint(0, 30),
            weapon_id=random.choice(["rifle_01", "sniper_01", "pistol_01"]),
            team_id=random.randint(1, 2),
            is_alive=not is_death,
            kills=player_data['kills'],
            deaths=player_data['deaths'],
            assists=random.randint(0, 5),
            damage_dealt=random.randint(50, 150) if is_kill else 0,
            damage_taken=random.randint(0, 50) if is_death else 0,
            movement_speed=movement_speed,
            reaction_time_ms=reaction_time,
            accuracy=accuracy,
            headshot_ratio=headshot_ratio,
            view_angle_change=view_angle_change,
            distance_traveled=random.uniform(0, 20) * speed_factor,
        )
    
    def send_event(self, event: GameEvent):
        """Send event to Kafka."""
        try:
            future = self.producer.send(
                self.topic,
                key=event.player_id,
                value=event.to_json()
            )
            record_metadata = future.get(timeout=10)
            print(f"Event sent: {event.event_id[:8]}... player={event.player_id} "
                  f"topic={record_metadata.topic} partition={record_metadata.partition}")
        except KafkaError as e:
            print(f"Failed to send event: {e}")
    
    def run_simulation(self, 
                       num_normal_players: int = 10,
                       num_cheaters: int = 2,
                       events_per_second: float = 5.0,
                       duration_seconds: int = 300):
        """Run the telemetry simulation."""
        print(f"Starting GameGuard telemetry generator...")
        print(f"Normal players: {num_normal_players}")
        print(f"Cheaters: {num_cheaters}")
        print(f"Target rate: {events_per_second} events/second")
        print(f"Duration: {duration_seconds} seconds")
        print("-" * 60)
        
        # Initialize players
        players = []
        
        # Create normal players
        for _ in range(num_normal_players):
            players.append({
                'data': self.create_normal_player_session(),
                'is_cheater': False,
                'cheat_type': None
            })
        
        # Create cheaters with different cheat types
        cheat_types = ["aimbot", "speedhack", "wallhack"]
        for i in range(num_cheaters):
            cheat_type = cheat_types[i % len(cheat_types)]
            players.append({
                'data': self.create_cheater_session(cheat_type),
                'is_cheater': True,
                'cheat_type': cheat_type
            })
        
        # Calculate delay between events
        delay_between_events = 1.0 / events_per_second
        
        start_time = time.time()
        event_count = 0
        
        try:
            while time.time() - start_time < duration_seconds:
                # Generate one event from a random player
                player = random.choice(players)
                
                if player['is_cheater']:
                    event = self.generate_cheater_event(player['data'], player['cheat_type'])
                else:
                    event = self.generate_normal_event(player['data'])
                
                self.send_event(event)
                event_count += 1
                
                # Sleep to maintain target rate
                time.sleep(delay_between_events)
                
        except KeyboardInterrupt:
            print("\nSimulation interrupted by user")
        finally:
            self.producer.flush()
            self.producer.close()
            print(f"\nSimulation complete. Total events sent: {event_count}")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='GameGuard Telemetry Generator')
    parser.add_argument('--kafka-servers', type=str, default='localhost:9092',
                        help='Kafka bootstrap servers (comma-separated)')
    parser.add_argument('--topic', type=str, default='raw.telemetry',
                        help='Kafka topic for telemetry events')
    parser.add_argument('--normal-players', type=int, default=10,
                        help='Number of normal players')
    parser.add_argument('--cheaters', type=int, default=2,
                        help='Number of cheaters')
    parser.add_argument('--events-per-second', type=float, default=5.0,
                        help='Target events per second')
    parser.add_argument('--duration', type=int, default=300,
                        help='Simulation duration in seconds')
    
    args = parser.parse_args()
    
    kafka_servers = [s.strip() for s in args.kafka_servers.split(',')]
    
    generator = TelemetryGenerator(
        kafka_bootstrap_servers=kafka_servers,
        topic=args.topic
    )
    
    generator.run_simulation(
        num_normal_players=args.normal_players,
        num_cheaters=args.cheaters,
        events_per_second=args.events_per_second,
        duration_seconds=args.duration
    )


if __name__ == '__main__':
    main()
